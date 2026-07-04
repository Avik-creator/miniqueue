package miniqueue

import "context"

// Handler is the interface that job processors must implement.
// It mirrors the shape of http.Handler — familiar, composable, and testable.
//
// Return nil to mark the job as completed.
// Return an error to mark the job as failed (it will be retried in Phase 3).
//
// The context is cancelled when:
//   - The lease is lost (another worker or the reaper reclaimed the job)
//   - The worker is shutting down
//
// Handlers should check ctx.Done() periodically and return promptly
// when cancelled to avoid doing work on a job that's no longer theirs.
type Handler interface {
	HandleJob(ctx context.Context, job *Job) error
}

// HandlerFunc is an adapter to allow ordinary functions as Handlers.
// Like http.HandlerFunc — keeps the API surface minimal.
type HandlerFunc func(ctx context.Context, job *Job) error

func (f HandlerFunc) HandleJob(ctx context.Context, job *Job) error {
	return f(ctx, job)
}