package miniqueue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// WorkerConfig configures a Worker.
// All fields have sensible defaults — only Queue and WorkerID are required.
type WorkerConfig struct {
	// Queue to consume from (required).
	Queue string

	// WorkerID uniquely identifies this worker. Used for lease ownership.
	// If two workers share the same ID, they'll fight over leases.
	// Use hostname+pid or a UUID.
	WorkerID string

	// PollInterval is how often the worker polls for new jobs when the
	// queue is empty. Default: 1s. This is our v1 tradeoff — Phase 4
	// replaces polling with LISTEN/NOTIFY for sub-millisecond latency.
	PollInterval time.Duration

	// LeaseDuration is how long a claimed job's lease lasts.
	// The worker must complete or renew before this expires.
	// Default: 30s.
	LeaseDuration time.Duration

	// HeartbeatInterval is how often the worker renews the lease.
	// Default: LeaseDuration / 3. Shorter = more DB load but safer.
	// Longer = fewer renewals but bigger crash window.
	HeartbeatInterval time.Duration

	// Concurrency is the number of jobs processed simultaneously.
	// Default: 1. Each concurrent slot runs its own claim-process cycle.
	Concurrency int

	// ShutdownTimeout is how long Start() waits for in-flight jobs
	// to finish after the context is cancelled. Default: 10s.
	// Jobs still running after the timeout are abandoned (the reaper
	// will recover them when their lease expires).
	ShutdownTimeout time.Duration

	// Logger for operational messages. Default: slog.Default().
	Logger *slog.Logger
}

// Worker consumes jobs from a single queue.
// It handles claiming, heartbeating, processing, and graceful shutdown.
type Worker struct {
	config  WorkerConfig
	store   *Store
	handler Handler
	log     *slog.Logger
}

// NewWorker creates a Worker. Call Start() to begin processing.
func NewWorker(store *Store, handler Handler, config WorkerConfig) *Worker {
	// Apply defaults.
	if config.PollInterval <= 0 {
		config.PollInterval = 1 * time.Second
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = DefaultLeaseDuration
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = config.LeaseDuration / 3
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 10 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	config.Logger = config.Logger.With("worker_id", config.WorkerID, "queue", config.Queue)

	return &Worker{
		config:  config,
		store:   store,
		handler: handler,
		log:     config.Logger,
	}
}

// Start runs the claim/process loop and blocks until ctx is cancelled.
//
// Shutdown sequence:
//  1. ctx is cancelled (signal, parent, or test)
//  2. Claim loop stops — no new jobs are claimed
//  3. In-flight handlers continue until they return or ShutdownTimeout elapses
//  4. Start() returns
//
// Any jobs still running after ShutdownTimeout are abandoned — the reaper
// will recover them when their lease expires. This is the crash-safe
// guarantee: even if the worker vanishes entirely, no jobs are lost.
func (w *Worker) Start(ctx context.Context) error {
	if w.config.Queue == "" {
		return errors.New("miniqueue: worker queue is required")
	}
	if w.config.WorkerID == "" {
		return errors.New("miniqueue: worker ID is required")
	}

	sem := make(chan struct{}, w.config.Concurrency)
	var wg sync.WaitGroup

	w.log.Info("worker starting",
		"concurrency", w.config.Concurrency,
		"poll_interval", w.config.PollInterval,
		"lease_duration", w.config.LeaseDuration,
		"heartbeat_interval", w.config.HeartbeatInterval,
	)

loop:
	for {
		// Acquire a processing slot. Blocks if all slots are in use.
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		}

		// Try to claim a job.
		job, err := w.store.Claim(ctx, w.config.Queue, w.config.WorkerID, w.config.LeaseDuration)
		if err != nil {
			<-sem // release the slot
			if errors.Is(err, ErrNoJobAvailable) {
				// Queue empty — wait before polling again.
				w.waitOrDone(ctx, w.config.PollInterval)
				continue
			}
			if ctx.Err() != nil {
				break loop
			}
			w.log.Error("claim failed", "error", err)
			w.waitOrDone(ctx, w.config.PollInterval)
			continue
		}

		// Claimed a job — dispatch to a goroutine.
		wg.Add(1)
		go w.processJob(ctx, &wg, job, sem)
	}

	w.log.Info("worker shutting down, draining in-flight jobs")

	// Wait for in-flight jobs with a timeout.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		w.log.Info("all in-flight jobs drained")
	case <-time.After(w.config.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached, abandoning in-flight jobs")
	}

	return nil
}

// processJob runs the handler with a heartbeat and records the outcome.
// It releases the semaphore slot and decrements the WaitGroup on return.
func (w *Worker) processJob(parentCtx context.Context, wg *sync.WaitGroup, job *Job, sem chan struct{}) {
	defer wg.Done()
	defer func() { <-sem }() // release processing slot

	w.log.Info("processing job", "job_id", job.ID, "attempt", job.Attempt)

	// Create a cancellable context for this specific job.
	// It's cancelled when: parent shuts down, heartbeat detects lost lease,
	// or the handler returns (deferred cancel).
	jobCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Start the heartbeat goroutine.
	go w.heartbeat(jobCtx, cancel, job.ID)

	// Run the user's handler.
	handlerErr := w.handler.HandleJob(jobCtx, job)

	// Stop the heartbeat before recording outcome.
	cancel()

	// Use a detached context for the completion/failure call.
	// The parent context may already be cancelled (shutdown), but we
	// still need to record the outcome. If this fails, the reaper
	// will recover the job when the lease expires.
	doneCtx, doneCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer doneCancel()

	if handlerErr != nil {
		w.log.Info("job failed", "job_id", job.ID, "error", handlerErr)
		if err := w.store.Fail(doneCtx, job.ID, w.config.WorkerID, handlerErr.Error()); err != nil {
			w.log.Error("failed to record job failure", "job_id", job.ID, "error", err)
		}
	} else {
		w.log.Info("job completed", "job_id", job.ID)
		if err := w.store.Complete(doneCtx, job.ID, w.config.WorkerID); err != nil {
			w.log.Error("failed to record job completion", "job_id", job.ID, "error", err)
		}
	}
}

// heartbeat periodically renews the lease on a running job.
// If the renewal fails (lease was stolen by the reaper or another worker),
// it cancels the job context to signal the handler to abort.
func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, jobID int64) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := w.store.RenewLease(ctx, jobID, w.config.WorkerID, w.config.LeaseDuration)
			if err != nil {
				w.log.Warn("heartbeat failed — lease may be lost",
					"job_id", jobID, "error", err)
				cancel() // signal handler to abort
				return
			}
		}
	}
}

// waitOrDone waits for the given duration or until ctx is cancelled.
func (w *Worker) waitOrDone(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}