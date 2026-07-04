package miniqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// benchDB returns a pgxpool connected to the test database for benchmarks.
// Reuses the same TEST_DATABASE_URL env var as integration tests.
func benchDB(b *testing.B) *pgxpool.Pool {
	b.Helper()
	dsn := getTestDSN()
	if dsn == "" {
		b.Skip("TEST_DATABASE_URL not set — skipping benchmark")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatalf("failed to connect to test database: %v", err)
	}

	if err := RunMigrations(ctx, pool, "migrations"); err != nil {
		b.Fatalf("failed to run migrations: %v", err)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE miniqueue_jobs RESTART IDENTITY"); err != nil {
		b.Fatalf("failed to truncate jobs: %v", err)
	}

	b.Cleanup(func() { pool.Close() })
	return pool
}

// BenchmarkEnqueue measures raw insert throughput.
func BenchmarkEnqueue(b *testing.B) {
	pool := benchDB(b)
	store := NewStore(pool)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Enqueue(ctx, EnqueueOptions{
			Queue:   "bench",
			Payload: json.RawMessage(`{"i":1}`),
		})
		if err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}
}

// BenchmarkClaim measures claim throughput under contention.
// It pre-loads jobs and then runs W concurrent workers claiming jobs.
func BenchmarkClaim(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
			pool := benchDB(b)
			store := NewStore(pool)
			ctx := context.Background()

			// Pre-load jobs.
			for i := 0; i < b.N; i++ {
				_, err := store.Enqueue(ctx, EnqueueOptions{
					Queue:   "bench",
					Payload: json.RawMessage(`{"i":1}`),
				})
				if err != nil {
					b.Fatalf("enqueue: %v", err)
				}
			}

			b.ResetTimer()

			done := make(chan struct{})
			for w := 0; w < workers; w++ {
				go func(workerID string) {
					for {
						_, err := store.Claim(ctx, "bench", workerID, 30*time.Second)
						if err != nil {
							break
						}
					}
					done <- struct{}{}
				}(fmt.Sprintf("worker-%d", w))
			}

			for w := 0; w < workers; w++ {
				<-done
			}
		})
	}
}

// BenchmarkClaimLatency measures the latency distribution of claim operations.
func BenchmarkClaimLatency(b *testing.B) {
	pool := benchDB(b)
	store := NewStore(pool)
	ctx := context.Background()

	// Pre-load jobs.
	for i := 0; i < b.N; i++ {
		_, err := store.Enqueue(ctx, EnqueueOptions{
			Queue:   "bench",
			Payload: json.RawMessage(`{"i":1}`),
		})
		if err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := store.Claim(ctx, "bench", "bench-worker", 30*time.Second)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("claim: %v", err)
		}
		b.ReportMetric(float64(elapsed.Microseconds()), "latency_µs")
	}
}

// BenchmarkEnqueueWithNotify measures enqueue throughput when the producer
// also sends a LISTEN/NOTIFY notification after each insert.
func BenchmarkEnqueueWithNotify(b *testing.B) {
	pool := benchDB(b)
	store := NewStore(pool)
	notifier := NewNotifier(pool, "bench")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Enqueue(ctx, EnqueueOptions{
			Queue:   "bench",
			Payload: json.RawMessage(`{"i":1}`),
		})
		if err != nil {
			b.Fatalf("enqueue: %v", err)
		}
		_ = notifier.Notify(ctx)
	}
}

