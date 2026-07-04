package miniqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Notifier uses PostgreSQL's LISTEN/NOTIFY to provide push-based wake
// when new jobs are enqueued. This is a latency optimization over polling:
//
//   - Without Notifier: worker waits up to PollInterval before seeing a new job.
//     Worst case latency = PollInterval (default 1s).
//
//   - With Notifier: worker is woken immediately when a job is enqueued.
//     Worst case latency = network round-trip (sub-millisecond on localhost).
//
// LISTEN/NOTIFY is NOT a correctness requirement — it's purely a performance
// optimization. If the notification is lost (connection drop, Postgres restart),
// the worker falls back to polling and eventually picks up the job.
//
// The Notifier uses a dedicated database connection for LISTEN, separate from
// the worker's connection pool. This is required because LISTEN blocks the
// connection — you can't run other queries on a connection that's waiting
// for notifications.
type Notifier struct {
	pool    *pgxpool.Pool
	channel string
	log     *slog.Logger
}

// NewNotifier creates a Notifier that listens on the given channel.
// The channel name should match the queue name for per-queue notifications,
// or use a shared channel name for all queues.
func NewNotifier(pool *pgxpool.Pool, channel string) *Notifier {
	if channel == "" {
		channel = "miniqueue_jobs"
	}
	return &Notifier{
		pool:    pool,
		channel: channel,
		log:     slog.Default().With("component", "notifier", "channel", channel),
	}
}

// Notify sends a notification on the configured channel.
// Called by the Store after a successful enqueue. This wakes any
// listening workers so they can immediately try to claim a job
// instead of waiting for the next poll interval.
func (n *Notifier) Notify(ctx context.Context) error {
	_, err := n.pool.Exec(ctx, fmt.Sprintf("SELECT pg_notify('%s', '')", n.channel))
	if err != nil {
		return fmt.Errorf("miniqueue: notify: %w", err)
	}
	return nil
}

// Listen blocks until a notification is received or ctx is cancelled.
// Returns the notification payload (or nil if cancelled).
//
// This uses a dedicated connection from the pool, acquires it for the
// duration of the listen call, then releases it.
func (n *Notifier) Listen(ctx context.Context) (*pgconn.Notification, error) {
	// Acquire a dedicated connection for LISTEN.
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("miniqueue: listen acquire: %w", err)
	}
	defer conn.Release()

	// Start listening on the channel.
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", n.channel))
	if err != nil {
		return nil, fmt.Errorf("miniqueue: listen: %w", err)
	}

	// Block until notification or context cancellation.
	notification, err := conn.Conn().WaitForNotification(ctx)
	if err != nil {
		// Context cancellation is expected during shutdown.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("miniqueue: wait for notification: %w", err)
	}

	return notification, nil
}

// WaitForWake returns a channel that receives when a notification arrives.
// The channel is closed when ctx is cancelled.
//
// This is the method the worker uses in its wait loop. It replaces the
// pure time.After() polling with a hybrid approach: wake on notification
// OR on poll interval, whichever comes first.
func (n *Notifier) WaitForWake(ctx context.Context) <-chan struct{} {
	wake := make(chan struct{}, 1)

	go func() {
		defer close(wake)
		for {
			notification, err := n.Listen(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // shutdown
				}
				n.log.Error("listen error, retrying", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			_ = notification

			// Non-blocking send — if the worker is already processing,
			// don't block the listen goroutine.
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()

	return wake
}
