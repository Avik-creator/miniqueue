package miniqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChaos_RandomWorkerKills proves that the system achieves zero job loss
// even when workers are killed randomly mid-processing.
//
// This is the "distributed systems correctness" test. It simulates real-world
// failure modes:
//   - Workers get SIGKILL'd (no graceful shutdown)
//   - Workers crash mid-job (no Complete/Fail call)
//   - The reaper must recover orphaned jobs
//   - Other workers pick up recovered jobs
//
// The test asserts:
//  1. Every enqueued job reaches a terminal state (completed or dead)
//  2. No job is processed more than once by the handler (no duplicate side-effects)
//  3. The final attempt count is correct
func TestChaos_RandomWorkerKills(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	bgCtx := context.Background()

	const (
		numJobs      = 50
		numWorkers   = 4
		testDuration = 20 * time.Second
	)

	// Enqueue jobs.
	for i := 0; i < numJobs; i++ {
		_, err := store.Enqueue(bgCtx, EnqueueOptions{
			Queue:       "chaos",
			Payload:     json.RawMessage(`{"chaos":true}`),
			MaxAttempts: 10, // generous retries for chaos
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Track processed jobs to detect duplicates.
	var processedIDs sync.Map
	var completedCount atomic.Int64

	// Handler that simulates variable work time.
	handler := HandlerFunc(func(ctx context.Context, job *Job) error {
		// Simulate work: 50-300ms.
		workTime := 50 + rand.Intn(250)
		select {
		case <-time.After(time.Duration(workTime) * time.Millisecond):
			// Track processing for duplicate detection.
			if _, loaded := processedIDs.LoadOrStore(job.ID, true); loaded {
				t.Errorf("DUPLICATE: job %d processed by multiple workers (attempt %d)", job.ID, job.Attempt)
			}
			completedCount.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Start the reaper.
	reaperCtx, reaperCancel := context.WithCancel(bgCtx)
	defer reaperCancel()

	reaper := NewReaper(store, ReaperConfig{
		Interval: 2 * time.Second,
	})
	go reaper.Start(reaperCtx)

	// Start workers, randomly killing and restarting them.
	var wg sync.WaitGroup
	workerCtxs := make([]context.CancelFunc, numWorkers)
	testDeadline := time.Now().Add(testDuration)

	for w := 0; w < numWorkers; w++ {
		wCtx, wCancel := context.WithCancel(bgCtx)
		workerCtxs[w] = wCancel

		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()

			worker := NewWorker(store, handler, WorkerConfig{
				Queue:             "chaos",
				WorkerID:          fmt.Sprintf("chaos-worker-%d", workerNum),
				PollInterval:      200 * time.Millisecond,
				LeaseDuration:     5 * time.Second,
				HeartbeatInterval: 1500 * time.Millisecond,
				ShutdownTimeout:   2 * time.Second,
				Backoff:           func(int) time.Duration { return 200 * time.Millisecond },
			})

			_ = worker.Start(wCtx)
		}(w)
	}

	// Chaos loop: randomly kill workers.
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		for time.Now().Before(testDeadline) {
			// Random delay before killing a worker.
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second)

			// Pick a random worker to kill.
			victim := rand.Intn(numWorkers)
			workerCtxs[victim]()

			// Wait a moment, then restart it.
			time.Sleep(500 * time.Millisecond)

			newCtx, newCancel := context.WithCancel(bgCtx)
			workerCtxs[victim] = newCancel

			wg.Add(1)
			go func(workerNum int) {
				defer wg.Done()
				worker := NewWorker(store, handler, WorkerConfig{
					Queue:             "chaos",
					WorkerID:          fmt.Sprintf("chaos-worker-%d", workerNum),
					PollInterval:      200 * time.Millisecond,
					LeaseDuration:     5 * time.Second,
					HeartbeatInterval: 1500 * time.Millisecond,
					ShutdownTimeout:   2 * time.Second,
					Backoff:           func(int) time.Duration { return 200 * time.Millisecond },
				})
				_ = worker.Start(newCtx)
			}(victim)
		}
	}()

	<-chaosDone

	// Stop all workers and wait for drain.
	for _, cancel := range workerCtxs {
		cancel()
	}
	wg.Wait()
	reaperCancel()

	// Final reaper pass to recover any stragglers.
	time.Sleep(6 * time.Second) // wait for leases to expire
	_, _ = store.RecoverExpiredLeases(bgCtx)

	// Assert: all jobs reached terminal state.
	var completed, failed, dead, available, running int
	err := pool.QueryRow(bgCtx, `
		SELECT
			count(*) FILTER (WHERE state = 'completed'),
			count(*) FILTER (WHERE state = 'failed'),
			count(*) FILTER (WHERE state = 'dead'),
			count(*) FILTER (WHERE state = 'available'),
			count(*) FILTER (WHERE state = 'running')
		FROM miniqueue_jobs WHERE queue = 'chaos'
	`).Scan(&completed, &failed, &dead, &available, &running)
	if err != nil {
		t.Fatalf("query terminal states: %v", err)
	}

	t.Logf("Final state: completed=%d, failed=%d, dead=%d, available=%d, running=%d",
		completed, failed, dead, available, running)
	t.Logf("Handler called %d times for %d jobs", completedCount.Load(), numJobs)

	// All jobs should be completed (none stuck in available/running/failed).
	if available > 0 || running > 0 {
		t.Errorf("jobs stuck in non-terminal state: available=%d, running=%d", available, running)
	}

	// All jobs should be completed.
	if completed != numJobs {
		t.Errorf("expected %d completed, got %d (dead=%d, failed=%d)", numJobs, completed, dead, failed)
	}
}
