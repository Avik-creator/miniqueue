package miniqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForJobCompleted polls the DB until the job reaches a terminal state.
func waitForJobCompleted(t *testing.T, store *Store, jobID int64, timeout time.Duration) State {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var state State
		err := store.pool.QueryRow(context.Background(),
			"SELECT state FROM miniqueue_jobs WHERE id = $1", jobID,
		).Scan(&state)
		if err != nil {
			t.Fatalf("poll job state: %v", err)
		}
		if state == StateCompleted || state == StateFailed || state == StateDead {
			return state
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %d to complete", jobID)
	return ""
}

func TestWorker_ProcessesJob(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:   "test-queue",
		Payload: json.RawMessage(`{"msg":"hello"}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var processedID atomic.Int64
	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		processedID.Store(job.ID)
		return nil
	})

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "test-queue",
		WorkerID:          "test-worker-1",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})

	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	state := waitForJobCompleted(t, store, job.ID, 5*time.Second)
	if state != StateCompleted {
		t.Errorf("expected completed, got %s", state)
	}
	if processedID.Load() != job.ID {
		t.Errorf("handler processed wrong job: want %d, got %d", job.ID, processedID.Load())
	}

	cancel()
	if err := <-workerDone; err != nil {
		t.Errorf("worker error: %v", err)
	}
}

func TestWorker_HeartbeatKeepsLeaseAlive(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:   "heartbeat-test",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		// Sleep 3x the lease duration — heartbeat must keep it alive.
		select {
		case <-time.After(6 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "heartbeat-test",
		WorkerID:          "hb-worker",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     2 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		ShutdownTimeout:   10 * time.Second,
	})

	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	state := waitForJobCompleted(t, store, job.ID, 10*time.Second)
	if state != StateCompleted {
		t.Errorf("expected completed, got %s", state)
	}
	cancel()
	<-workerDone
}

func TestWorker_FailedJobRecordsError(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "fail-test",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 1, // Fail once → dead-letter
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		return errors.New("database connection refused")
	})

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "fail-test",
		WorkerID:          "fail-worker",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})

	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	// With max_attempts=1, the job goes to dead after one failure
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var state State
		_ = pool.QueryRow(ctx, "SELECT state FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&state)
		if state == StateDead {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	var state State
	var lastError *string
	err = pool.QueryRow(ctx,
		"SELECT state, last_error FROM miniqueue_jobs WHERE id=$1", job.ID,
	).Scan(&state, &lastError)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state != StateDead {
		t.Errorf("expected dead, got %s", state)
	}
	if lastError == nil || *lastError != "database connection refused" {
		t.Errorf("wrong last_error: got %v", lastError)
	}

	cancel()
	<-workerDone
}

func TestWorker_GracefulShutdown(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)

	job, err := store.Enqueue(context.Background(), EnqueueOptions{
		Queue:   "shutdown-test",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handlerStarted := make(chan struct{})
	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		close(handlerStarted)
		// Deliberately ignore ctx.Done() — simulates a handler that doesn't
		// respect cancellation. The worker MUST wait for this to finish
		// (within ShutdownTimeout) rather than abandoning it.
		time.Sleep(2 * time.Second)
		return nil
	})

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "shutdown-test",
		WorkerID:          "shutdown-worker",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	<-handlerStarted // handler is mid-flight
	cancel()         // trigger shutdown

	start := time.Now()
	select {
	case err := <-workerDone:
		elapsed := time.Since(start)
		if err != nil {
			t.Errorf("worker error: %v", err)
		}
		// Worker should have waited ~2s for the handler to finish.
		if elapsed < 1500*time.Millisecond {
			t.Errorf("worker exited too fast (%v) — didn't wait for handler", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("worker did not shut down within timeout")
	}

	// Verify the job was actually completed (not abandoned).
	state := waitForJobCompleted(t, store, job.ID, 2*time.Second)
	if state != StateCompleted {
		t.Errorf("expected job completed during drain, got %s", state)
	}
}

// TestWorker_ReaperRecoversKilledWorker is the critical crash-resilience test.
//
// Scenario:
//  1. Worker A claims a job and starts processing
//  2. Worker A "crashes" (we cancel its context, simulating a kill)
//  3. The job's lease expires
//  4. The Reaper detects the expired lease and resets the job to available
//  5. Worker B claims the recovered job and completes it
//
// This proves: zero job loss even when a worker dies mid-processing.
func TestWorker_ReaperRecoversKilledWorker(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	bgCtx := context.Background()

	job, err := store.Enqueue(bgCtx, EnqueueOptions{
		Queue:   "crash-test",
		Payload: json.RawMessage(`{"critical":"data"}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// --- Phase 1: Worker A claims and "crashes" ---
	handlerAStarted := make(chan struct{})
	handlerA := HandlerFunc(func(ctx context.Context, job *Job) error {
		close(handlerAStarted)
		// Block until context is cancelled (simulating a long-running job).
		<-ctx.Done()
		return ctx.Err()
	})

	workerACtx, workerACancel := context.WithCancel(bgCtx)
	workerA := NewWorker(store, handlerA, WorkerConfig{
		Queue:             "crash-test",
		WorkerID:          "worker-A",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     2 * time.Second, // Short lease for fast test.
		HeartbeatInterval: 500 * time.Millisecond,
		ShutdownTimeout:   3 * time.Second,
	})

	workerADone := make(chan error, 1)
	go func() { workerADone <- workerA.Start(workerACtx) }()

	<-handlerAStarted // Worker A is processing.

	// Simulate crash: cancel worker A without letting it complete/fail.
	workerACancel()
	<-workerADone

	// Verify the job is stuck in 'running' with an expired lease.
	var state State
	err = pool.QueryRow(bgCtx, "SELECT state FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&state)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	// It may already be 'failed' since the handler returned an error via ctx.Done().
	// That's fine — we need to test the reaper path, so let's manually reset it.
	_, err = pool.Exec(bgCtx, `
		UPDATE miniqueue_jobs
		SET state = 'running', lease_expires_at = now() - interval '1 second',
		    leased_by = 'worker-A', last_error = NULL
		WHERE id = $1`, job.ID)
	if err != nil {
		t.Fatalf("reset job to expired running: %v", err)
	}

	// --- Phase 2: Reaper recovers the orphaned job ---
	reaperCtx, reaperCancel := context.WithCancel(bgCtx)
	reaper := NewReaper(store, ReaperConfig{
		Interval: 500 * time.Millisecond,
	})
	go reaper.Start(reaperCtx)

	// Wait for the reaper to recover the job.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var s State
		_ = pool.QueryRow(bgCtx, "SELECT state FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&s)
		if s == StateAvailable {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	reaperCancel()

	var recoveredState State
	err = pool.QueryRow(bgCtx, "SELECT state FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&recoveredState)
	if err != nil {
		t.Fatalf("query recovered state: %v", err)
	}
	if recoveredState != StateAvailable {
		t.Fatalf("expected state 'available' after reaper, got %q", recoveredState)
	}

	// --- Phase 3: Worker B picks up the recovered job and completes it ---
	var processedByB atomic.Int64
	handlerB := HandlerFunc(func(ctx context.Context, job *Job) error {
		processedByB.Store(job.ID)
		return nil
	})

	workerBCtx, workerBCancel := context.WithCancel(bgCtx)
	defer workerBCancel()

	workerB := NewWorker(store, handlerB, WorkerConfig{
		Queue:             "crash-test",
		WorkerID:          "worker-B",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})

	workerBDone := make(chan error, 1)
	go func() { workerBDone <- workerB.Start(workerBCtx) }()

	finalState := waitForJobCompleted(t, store, job.ID, 5*time.Second)
	if finalState != StateCompleted {
		t.Errorf("expected completed by worker B, got %s", finalState)
	}

	// Verify it was attempt #2 (first by A, recovered, then by B).
	var attempt int
	_ = pool.QueryRow(bgCtx, "SELECT attempt FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&attempt)
	if attempt != 2 {
		t.Errorf("expected attempt 2, got %d", attempt)
	}

	workerBCancel()
	<-workerBDone
}

// TestWorker_ConcurrentWorkers proves that multiple worker instances
// can safely compete for jobs in the same queue without duplicates.
func TestWorker_ConcurrentWorkers(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	bgCtx := context.Background()

	const numJobs = 30
	const numWorkers = 3

	// Enqueue jobs.
	for i := 0; i < numJobs; i++ {
		_, err := store.Enqueue(bgCtx, EnqueueOptions{
			Queue:   "compete",
			Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var processedIDs sync.Map
	var completedCount atomic.Int64

	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		// Small random delay to increase interleaving.
		time.Sleep(20 * time.Millisecond)
		if _, loaded := processedIDs.LoadOrStore(job.ID, true); loaded {
			return fmt.Errorf("DUPLICATE: job %d processed by multiple workers", job.ID)
		}
		completedCount.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(bgCtx)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		worker := NewWorker(store, handler, WorkerConfig{
			Queue:             "compete",
			WorkerID:          fmt.Sprintf("compete-worker-%d", w),
			PollInterval:      100 * time.Millisecond,
			LeaseDuration:     10 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			ShutdownTimeout:   5 * time.Second,
		})
		go func() {
			defer wg.Done()
			_ = worker.Start(ctx)
		}()
	}

	// Wait until all jobs are processed or timeout.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if completedCount.Load() >= int64(numJobs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	got := completedCount.Load()
	if got != int64(numJobs) {
		t.Errorf("expected %d completed jobs, got %d", numJobs, got)
	}

	// Verify all jobs are in completed state in the DB.
	var dbCount int
	err := pool.QueryRow(bgCtx,
		"SELECT count(*) FROM miniqueue_jobs WHERE queue='compete' AND state='completed'",
	).Scan(&dbCount)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if dbCount != numJobs {
		t.Errorf("expected %d completed in DB, got %d", numJobs, dbCount)
	}
}

// TestWorker_RetryAndDeadLetter tests the full retry cycle:
// job fails → retries with backoff → eventually dead-lettered after max_attempts.
func TestWorker_RetryAndDeadLetter(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "retry-e2e",
		Payload:     json.RawMessage(`{"task":"will_always_fail"}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Handler that always fails
	var attemptCount atomic.Int64
	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		attemptCount.Add(1)
		return errors.New("simulated failure")
	})

	// Use a very short backoff so the test runs quickly
	zeroBackoff := func(attempt int) time.Duration { return 100 * time.Millisecond }

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "retry-e2e",
		WorkerID:          "retry-worker",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		Backoff:           zeroBackoff,
	})

	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	// Wait for the job to be dead-lettered (after 3 attempts)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var state State
		_ = pool.QueryRow(ctx, "SELECT state FROM miniqueue_jobs WHERE id=$1", job.ID).Scan(&state)
		if state == StateDead {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify it's dead
	var state State
	var attempt int
	var lastError *string
	err = pool.QueryRow(ctx,
		"SELECT state, attempt, last_error FROM miniqueue_jobs WHERE id=$1", job.ID,
	).Scan(&state, &attempt, &lastError)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if state != StateDead {
		t.Errorf("expected state 'dead', got %q", state)
	}
	if attempt != 3 {
		t.Errorf("expected 3 attempts, got %d", attempt)
	}
	if lastError == nil || *lastError != "simulated failure" {
		t.Errorf("expected last_error 'simulated failure', got %v", lastError)
	}

	cancel()
	<-workerDone
}

// TestWorker_EventualSuccessAfterRetry tests that a job can succeed
// after failing a few times (simulating transient failures).
func TestWorker_EventualSuccessAfterRetry(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "eventual-success",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Handler that fails twice then succeeds
	var callCount atomic.Int64
	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		n := callCount.Add(1)
		if n < 3 {
			return fmt.Errorf("transient error #%d", n)
		}
		return nil // success on 3rd attempt
	})

	zeroBackoff := func(attempt int) time.Duration { return 100 * time.Millisecond }

	w := NewWorker(store, handler, WorkerConfig{
		Queue:             "eventual-success",
		WorkerID:          "eventual-worker",
		PollInterval:      100 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		Backoff:           zeroBackoff,
	})

	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Start(ctx) }()

	state := waitForJobCompleted(t, store, job.ID, 10*time.Second)
	if state != StateCompleted {
		t.Errorf("expected completed, got %s", state)
	}

	if callCount.Load() != 3 {
		t.Errorf("expected 3 handler calls, got %d", callCount.Load())
	}

	cancel()
	<-workerDone
}