// Package miniqueue is a Postgres-backed job queue with lease-based semantics
// and at-least-once delivery guarantees via consumer-side idempotency keys.
//
// Design philosophy:
//   - Lease-based claim model: workers acquire a time-bounded lease on a job.
//     If the worker crashes before completing the job, the lease expires and
//     another worker (or the reaper) can reclaim it.
//   - At-least-once delivery: network partitions and crash windows between
//     COMMIT and ACK make true exactly-once delivery impossible without
//     distributed consensus. Instead, we guarantee at-least-once and rely
//     on idempotency keys for deduplication on the consumer side.
//   - FOR UPDATE SKIP LOCKED for concurrent claim: avoids busy-wait errors
//     and lets multiple workers efficiently compete for jobs.
package miniqueue

import (
	"encoding/json"
	"time"
)

// State represents the lifecycle state of a job.
type State string

const (
	StateAvailable State = "available"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateDead      State = "dead"
)

// Job represents a unit of work stored in the queue.
type Job struct {
	// ID is the auto-generated unique identifier.
	ID int64 `json:"id"`

	// Queue is the named queue this job belongs to.
	Queue string `json:"queue"`

	// IdempotencyKey is an optional caller-supplied dedup key.
	// If set, enqueueing a job with a duplicate key is a no-op (returns the existing job).
	// This is how we achieve at-least-once semantics without double-execution:
	// the consumer provides a unique key per logical operation.
	IdempotencyKey *string `json:"idempotency_key,omitempty"`

	// Payload is the arbitrary JSON data the worker needs to process the job.
	Payload json.RawMessage `json:"payload"`

	// State is the current lifecycle state.
	State State `json:"state"`

	// Priority controls claim ordering. Higher values are claimed first.
	// Within the same priority, jobs are claimed in scheduled_at order (FIFO).
	Priority int16 `json:"priority"`

	// Attempt is the number of times this job has been claimed/attempted.
	// Starts at 0; incremented on each claim.
	Attempt int `json:"attempt"`

	// MaxAttempts is the ceiling before the job transitions to "dead".
	MaxAttempts int `json:"max_attempts"`

	// ScheduledAt is the earliest time this job should be claimed.
	// Use this for delayed/scheduled jobs. Defaults to now().
	ScheduledAt time.Time `json:"scheduled_at"`

	// LeaseExpiresAt is when the current lease expires.
	// NULL when the job is not running.
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	// LeasedBy identifies the worker that holds the current lease.
	LeasedBy *string `json:"leased_by,omitempty"`

	// LastError contains the error message from the most recent failure.
	LastError *string `json:"last_error,omitempty"`

	// CreatedAt is when the job was enqueued.
	CreatedAt time.Time `json:"created_at"`

	// CompletedAt is when the job was successfully completed.
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// EnqueueOptions configures how a job is enqueued.
type EnqueueOptions struct {
	// Queue name (required).
	Queue string

	// Payload is the JSON-serializable job data (required).
	Payload json.RawMessage

	// IdempotencyKey for deduplication (optional).
	IdempotencyKey *string

	// Priority for claim ordering (optional, default 0).
	Priority int16

	// MaxAttempts before dead-lettering (optional, default 5).
	MaxAttempts int

	// ScheduledAt for delayed execution (optional, default now).
	ScheduledAt *time.Time
}
