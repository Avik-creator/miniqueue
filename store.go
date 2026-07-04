package miniqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNoJobAvailable is returned by Claim when no jobs are available in the queue.
var ErrNoJobAvailable = errors.New("miniqueue: no job available")

// ErrJobNotFound is returned when a job ID does not exist.
var ErrJobNotFound = errors.New("miniqueue: job not found")

// ErrJobNotLeased is returned when trying to complete/fail a job that isn't
// currently leased by the given worker. This prevents a stale worker from
// modifying a job that has already been reclaimed by someone else.
var ErrJobNotLeased = errors.New("miniqueue: job not leased by this worker")

// Store is the Postgres-backed storage layer for miniqueue.
// It owns all SQL queries and is the only component that touches the database.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a Store backed by an existing pgx connection pool.
// The caller is responsible for running migrations before using the Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Enqueue inserts a new job into the queue.
//
// If an IdempotencyKey is provided and a job with that key already exists,
// the existing job is returned without creating a duplicate. This is the
// mechanism that makes at-least-once delivery safe: even if a producer
// retries an enqueue (e.g., after a network timeout), the job is only
// stored once.
func (s *Store) Enqueue(ctx context.Context, opts EnqueueOptions) (*Job, error) {
	job, _, err := s.EnqueueWithResult(ctx, opts)
	return job, err
}

// EnqueueWithResult inserts a new job into the queue and reports whether it
// created a new row or reused an existing idempotent job.
func (s *Store) EnqueueWithResult(ctx context.Context, opts EnqueueOptions) (*Job, bool, error) {
	if opts.Queue == "" {
		return nil, false, errors.New("miniqueue: queue name is required")
	}
	if opts.Payload == nil {
		opts.Payload = json.RawMessage("{}")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}

	scheduledAt := time.Now()
	if opts.ScheduledAt != nil {
		scheduledAt = *opts.ScheduledAt
	}

	if opts.IdempotencyKey != nil {
		return s.enqueueIdempotent(ctx, opts, scheduledAt)
	}
	return s.enqueueSimple(ctx, opts, scheduledAt)
}

// enqueueIdempotent inserts with ON CONFLICT DO NOTHING and returns
// the existing job on conflict.
func (s *Store) enqueueIdempotent(ctx context.Context, opts EnqueueOptions, scheduledAt time.Time) (*Job, bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO miniqueue_jobs (queue, idempotency_key, payload, priority, max_attempts, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		opts.Queue, *opts.IdempotencyKey, opts.Payload,
		opts.Priority, opts.MaxAttempts, scheduledAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("miniqueue: enqueue: %w", err)
	}

	wasNew := tag.RowsAffected() == 1

	// Whether it was inserted or conflicted, fetch by key.
	job, err := s.fetchByIdempotencyKey(ctx, *opts.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	return job, wasNew, nil
}

// enqueueSimple inserts a job without an idempotency key.
func (s *Store) enqueueSimple(ctx context.Context, opts EnqueueOptions, scheduledAt time.Time) (*Job, bool, error) {
	var job Job
	err := s.pool.QueryRow(ctx, `
		INSERT INTO miniqueue_jobs (queue, payload, priority, max_attempts, scheduled_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, queue, idempotency_key, payload, state, priority,
		          attempt, max_attempts, scheduled_at, lease_expires_at,
		          leased_by, last_error, created_at, completed_at`,
		opts.Queue, opts.Payload, opts.Priority, opts.MaxAttempts, scheduledAt,
	).Scan(
		&job.ID, &job.Queue, &job.IdempotencyKey, &job.Payload, &job.State,
		&job.Priority, &job.Attempt, &job.MaxAttempts, &job.ScheduledAt,
		&job.LeaseExpiresAt, &job.LeasedBy, &job.LastError, &job.CreatedAt,
		&job.CompletedAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("miniqueue: enqueue: %w", err)
	}
	return &job, true, nil
}

// fetchByIdempotencyKey retrieves a job by its idempotency key.
func (s *Store) fetchByIdempotencyKey(ctx context.Context, key string) (*Job, error) {
	var job Job
	err := s.pool.QueryRow(ctx, `
		SELECT id, queue, idempotency_key, payload, state, priority,
		       attempt, max_attempts, scheduled_at, lease_expires_at,
		       leased_by, last_error, created_at, completed_at
		FROM miniqueue_jobs
		WHERE idempotency_key = $1`, key,
	).Scan(
		&job.ID, &job.Queue, &job.IdempotencyKey, &job.Payload, &job.State,
		&job.Priority, &job.Attempt, &job.MaxAttempts, &job.ScheduledAt,
		&job.LeaseExpiresAt, &job.LeasedBy, &job.LastError, &job.CreatedAt,
		&job.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("miniqueue: fetch by idempotency key: %w", err)
	}
	return &job, nil
}

// Claim atomically claims the next available job in the given queue.
//
// This is the single most important query in the system. Here's why each
// part matters:
//
//   - FOR UPDATE: takes a row-level lock so no other transaction can claim
//     the same job concurrently.
//
//   - SKIP LOCKED: if a row is already locked by another transaction, skip
//     it and move to the next one. Critical for throughput under contention —
//     without it, workers block on each other or get serialization errors.
//     We chose SKIP LOCKED over NOWAIT because NOWAIT throws an error on
//     lock conflict, requiring error handling per poll. SKIP LOCKED just
//     moves on, which is the behavior we want with competing workers.
//
//   - ORDER BY priority DESC, scheduled_at: higher priority first. Within
//     the same priority, earlier scheduled_at wins (FIFO). Under concurrent
//     claim with SKIP LOCKED, strict FIFO is only guaranteed with a single
//     worker. True per-queue FIFO under concurrency would require serializable
//     isolation, which kills throughput. Acceptable tradeoff.
//
//   - Lease model: the job gets a time-bounded lease. If the worker crashes
//     before calling Complete/Fail, the lease expires and the reaper resets
//     the job to available.
func (s *Store) Claim(ctx context.Context, queue, workerID string, leaseDuration time.Duration) (*Job, error) {
	var job Job
	err := s.pool.QueryRow(ctx, `
		UPDATE miniqueue_jobs
		SET state = 'running',
		    leased_by = $1,
		    lease_expires_at = now() + $2::interval,
		    attempt = attempt + 1
		WHERE id = (
		    SELECT id FROM miniqueue_jobs
		    WHERE queue = $3
		      AND state = 'available'
		      AND scheduled_at <= now()
		    ORDER BY priority DESC, scheduled_at
		    FOR UPDATE SKIP LOCKED
		    LIMIT 1
		)
		RETURNING id, queue, idempotency_key, payload, state, priority,
		          attempt, max_attempts, scheduled_at, lease_expires_at,
		          leased_by, last_error, created_at, completed_at`,
		workerID, leaseDuration.String(), queue,
	).Scan(
		&job.ID, &job.Queue, &job.IdempotencyKey, &job.Payload, &job.State,
		&job.Priority, &job.Attempt, &job.MaxAttempts, &job.ScheduledAt,
		&job.LeaseExpiresAt, &job.LeasedBy, &job.LastError, &job.CreatedAt,
		&job.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoJobAvailable
		}
		return nil, fmt.Errorf("miniqueue: claim: %w", err)
	}
	return &job, nil
}

// Complete marks a job as successfully completed.
// Only the worker holding the current lease can complete a job.
func (s *Store) Complete(ctx context.Context, jobID int64, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET state = 'completed',
		    completed_at = now(),
		    lease_expires_at = NULL,
		    leased_by = NULL
		WHERE id = $1
		  AND state = 'running'
		  AND leased_by = $2`,
		jobID, workerID,
	)
	if err != nil {
		return fmt.Errorf("miniqueue: complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.diagnoseMissedUpdate(ctx, jobID, workerID)
	}
	return nil
}

// Fail marks a job as failed with the given error message.
// The job transitions to 'failed' and can be requeued manually if it should
// be retried later. Only the worker holding the current lease can fail a job.
func (s *Store) Fail(ctx context.Context, jobID int64, workerID string, errMsg string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET state = 'failed',
		    last_error = $3,
		    lease_expires_at = NULL,
		    leased_by = NULL
		WHERE id = $1
		  AND state = 'running'
		  AND leased_by = $2`,
		jobID, workerID, errMsg,
	)
	if err != nil {
		return fmt.Errorf("miniqueue: fail: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.diagnoseMissedUpdate(ctx, jobID, workerID)
	}
	return nil
}

// RenewLease extends the lease on a running job.
// Workers call this periodically (heartbeat) to prevent the reaper
// from reclaiming their job during long-running tasks.
// Only the worker holding the current lease can renew it.
func (s *Store) RenewLease(ctx context.Context, jobID int64, workerID string, extendBy time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET lease_expires_at = now() + $3::interval
		WHERE id = $1
		  AND state = 'running'
		  AND leased_by = $2`,
		jobID, workerID, extendBy.String(),
	)
	if err != nil {
		return fmt.Errorf("miniqueue: renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.diagnoseMissedUpdate(ctx, jobID, workerID)
	}
	return nil
}

// RecoverExpiredLeases resets running jobs whose lease has expired back to
// 'available'. This is the crash recovery mechanism: if a worker crashes
// (SIGKILL, OOM, network partition) without calling Complete or Fail,
// its leased jobs eventually expire. The reaper goroutine calls this
// periodically to make those jobs claimable again.
//
// Returns the number of jobs recovered.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET state = 'available',
		    leased_by = NULL,
		    lease_expires_at = NULL
		WHERE state = 'running'
		  AND lease_expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("miniqueue: recover expired leases: %w", err)
	}
	return tag.RowsAffected(), nil
}

// diagnoseMissedUpdate figures out why an update affected 0 rows.
// Distinguishes "job doesn't exist" from "lease was stolen by another worker".
func (s *Store) diagnoseMissedUpdate(ctx context.Context, jobID int64, workerID string) error {
	var state State
	var leasedBy *string
	err := s.pool.QueryRow(ctx, `
		SELECT state, leased_by FROM miniqueue_jobs WHERE id = $1`, jobID,
	).Scan(&state, &leasedBy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrJobNotFound
		}
		return fmt.Errorf("miniqueue: diagnose: %w", err)
	}
	return fmt.Errorf("%w: job %d is in state %q, leased by %v", ErrJobNotLeased, jobID, state, leasedBy)
}

// RecordFailure marks a job as failed and either schedules it for retry or
// moves it to the dead-letter state based on the attempt count.
//
// If attempt < max_attempts:
//   - State transitions to 'available'
//   - scheduled_at is set to now() + backoff(attempt)
//   - The job will be re-claimed after the backoff period
//
// If attempt >= max_attempts:
//   - State transitions to 'dead'
//   - The job is permanently failed and requires manual intervention
//
// The backoff function is called with the current attempt number (1-indexed).
// Only the worker holding the current lease can record a failure.
func (s *Store) RecordFailure(ctx context.Context, jobID int64, workerID string, errMsg string, backoff BackoffFunc) error {
	// Start a transaction to ensure consistency
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("miniqueue: record failure begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch the current job state
	var job Job
	err = tx.QueryRow(ctx, `
		SELECT id, queue, idempotency_key, payload, state, priority,
		       attempt, max_attempts, scheduled_at, lease_expires_at,
		       leased_by, last_error, created_at, completed_at
		FROM miniqueue_jobs
		WHERE id = $1
		  AND state = 'running'
		  AND leased_by = $2
		FOR UPDATE`, jobID, workerID,
	).Scan(
		&job.ID, &job.Queue, &job.IdempotencyKey, &job.Payload, &job.State,
		&job.Priority, &job.Attempt, &job.MaxAttempts, &job.ScheduledAt,
		&job.LeaseExpiresAt, &job.LeasedBy, &job.LastError, &job.CreatedAt,
		&job.CompletedAt,
	)
	if err != nil {
		tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return s.diagnoseMissedUpdate(ctx, jobID, workerID)
		}
		return fmt.Errorf("miniqueue: record failure fetch: %w", err)
	}

	// Decide: retry or dead-letter?
	var newState State
	var scheduledAt time.Time
	if job.Attempt >= job.MaxAttempts {
		newState = StateDead
		scheduledAt = job.ScheduledAt // keep original scheduled_at for dead jobs
	} else {
		newState = StateAvailable
		backoffDuration := backoff(job.Attempt)
		scheduledAt = time.Now().Add(backoffDuration)
	}

	// Update the job
	_, err = tx.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET state = $1,
		    last_error = $2,
		    scheduled_at = $3,
		    lease_expires_at = NULL,
		    leased_by = NULL
		WHERE id = $4`,
		newState, errMsg, scheduledAt, jobID,
	)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("miniqueue: record failure update: %w", err)
	}

	return tx.Commit(ctx)
}

// Requeue manually moves a failed or dead-letter job back to the available state.
// This is used to retry jobs that were permanently failed but can now be retried
// (e.g., after fixing a bug or restoring a dependency).
//
// Jobs in either the 'failed' or 'dead' state can be requeued. The scheduled_at
// is reset to now().
func (s *Store) Requeue(ctx context.Context, jobID int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE miniqueue_jobs
		SET state = 'available',
		    scheduled_at = now(),
		    lease_expires_at = NULL,
		    leased_by = NULL
		WHERE id = $1
		  AND state IN ('failed', 'dead')`,
		jobID,
	)
	if err != nil {
		return fmt.Errorf("miniqueue: requeue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Check why it failed
		var state State
		err := s.pool.QueryRow(ctx, `SELECT state FROM miniqueue_jobs WHERE id = $1`, jobID).Scan(&state)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrJobNotFound
			}
			return fmt.Errorf("miniqueue: requeue diagnose: %w", err)
		}
		return fmt.Errorf("miniqueue: cannot requeue job %d in state %q (must be 'failed' or 'dead')", jobID, state)
	}
	return nil
}
