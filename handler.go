package miniqueue

import "context"

// Handler is the interface that job processors must implement.
// It mirrors the shape of http.Handler — familiar, composable, and testable.
//
// Return nil to mark the job as completed.
// Return an error to mark the job as failed. The worker records the error,
// then either retries the job later or dead-letters it when its attempt budget
// is exhausted.
//
// The context is cancelled when:
//   - The lease is lost (another worker or the reaper reclaimed the job)
//   - The worker is shutting down
//
// Cancellation is cooperative, not forceful. Handlers should check ctx.Done()
// periodically and return promptly when cancelled, but a handler that ignores
// cancellation may continue running until it finishes on its own.
type Handler interface {
	HandleJob(ctx context.Context, job *Job) error
}

// HandlerFunc is an adapter to allow ordinary functions as Handlers.
// Like http.HandlerFunc — keeps the API surface minimal.
type HandlerFunc func(ctx context.Context, job *Job) error

func (f HandlerFunc) HandleJob(ctx context.Context, job *Job) error {
	return f(ctx, job)
}
