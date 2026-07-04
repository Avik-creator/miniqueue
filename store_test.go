package miniqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// getTestDSN returns the test database DSN from the environment.
// Shared by both integration tests and benchmarks.
func getTestDSN() string {
	return os.Getenv("TEST_DATABASE_URL")
}

// testDB returns a pgxpool connected to the test database.
// Set TEST_DATABASE_URL to a Postgres connection string.
// Tests are skipped if the env var is not set.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}

	if err := RunMigrations(ctx, pool, "migrations"); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE miniqueue_jobs RESTART IDENTITY"); err != nil {
		t.Fatalf("failed to truncate jobs: %v", err)
	}

	t.Cleanup(func() { pool.Close() })
	return pool
}

func TestEnqueue_Simple(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:   "emails",
		Payload: json.RawMessage(`{"to":"alice@example.com"}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if job.ID == 0 {
		t.Error("expected non-zero job ID")
	}
	if job.Queue != "emails" {
		t.Errorf("expected queue 'emails', got %q", job.Queue)
	}
	if job.State != StateAvailable {
		t.Errorf("expected state 'available', got %q", job.State)
	}
	if job.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", job.Attempt)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("expected max_attempts 5, got %d", job.MaxAttempts)
	}
}

func TestEnqueue_IdempotencyKey_NoDuplicates(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	key := "order-12345"
	opts := EnqueueOptions{
		Queue:          "orders",
		Payload:        json.RawMessage(`{"order_id":12345}`),
		IdempotencyKey: &key,
	}

	job1, err := store.Enqueue(ctx, opts)
	if err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	job2, err := store.Enqueue(ctx, opts)
	if err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}

	if job1.ID != job2.ID {
		t.Errorf("expected same job ID, got %d and %d", job1.ID, job2.ID)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM miniqueue_jobs").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 job in DB, got %d", count)
	}
}

func TestEnqueue_RequiresQueueName(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{Payload: json.RawMessage(`{}`)})
	if err == nil {
		t.Error("expected error for missing queue name")
	}
}

func TestClaim_Basic(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	enqueued, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:   "emails",
		Payload: json.RawMessage(`{"to":"bob"}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "emails", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claimed.ID != enqueued.ID {
		t.Errorf("claimed ID %d != enqueued ID %d", claimed.ID, enqueued.ID)
	}
	if claimed.State != StateRunning {
		t.Errorf("expected state 'running', got %q", claimed.State)
	}
	if claimed.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", claimed.Attempt)
	}
	if claimed.LeasedBy == nil || *claimed.LeasedBy != "worker-1" {
		t.Errorf("expected leased_by 'worker-1', got %v", claimed.LeasedBy)
	}
	if claimed.LeaseExpiresAt == nil {
		t.Error("expected non-nil lease_expires_at")
	}
}

func TestClaim_EmptyQueue(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Claim(ctx, "empty-queue", "worker-1", 30*time.Second)
	if !errors.Is(err, ErrNoJobAvailable) {
		t.Errorf("expected ErrNoJobAvailable, got %v", err)
	}
}

func TestClaim_PriorityOrdering(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	low, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "tasks", Payload: json.RawMessage(`{"name":"low"}`), Priority: 1,
	})
	if err != nil {
		t.Fatalf("enqueue low failed: %v", err)
	}
	high, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "tasks", Payload: json.RawMessage(`{"name":"high"}`), Priority: 10,
	})
	if err != nil {
		t.Fatalf("enqueue high failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "tasks", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claimed.ID != high.ID {
		t.Errorf("expected high-priority job %d, got %d", high.ID, claimed.ID)
	}

	claimed2, err := store.Claim(ctx, "tasks", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("second claim failed: %v", err)
	}
	if claimed2.ID != low.ID {
		t.Errorf("expected low-priority job %d, got %d", low.ID, claimed2.ID)
	}
}

func TestClaim_ScheduledInFuture(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	future := time.Now().Add(1 * time.Hour)
	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "delayed", Payload: json.RawMessage(`{}`), ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, err = store.Claim(ctx, "delayed", "worker-1", 30*time.Second)
	if !errors.Is(err, ErrNoJobAvailable) {
		t.Errorf("expected ErrNoJobAvailable for future job, got %v", err)
	}
}

func TestComplete(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "emails", Payload: json.RawMessage(`{"to":"carol"}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "emails", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	if err := store.Complete(ctx, claimed.ID, "worker-1"); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	var state State
	var completedAt *time.Time
	err = pool.QueryRow(ctx, `SELECT state, completed_at FROM miniqueue_jobs WHERE id = $1`, claimed.ID).Scan(&state, &completedAt)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if state != StateCompleted {
		t.Errorf("expected state 'completed', got %q", state)
	}
	if completedAt == nil {
		t.Error("expected non-nil completed_at")
	}
}

func TestComplete_WrongWorker(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "emails", Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "emails", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	err = store.Complete(ctx, claimed.ID, "worker-2")
	if !errors.Is(err, ErrJobNotLeased) {
		t.Errorf("expected ErrJobNotLeased, got %v", err)
	}
}

func TestFail(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "emails", Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "emails", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	if err := store.Fail(ctx, claimed.ID, "worker-1", "SMTP connection refused"); err != nil {
		t.Fatalf("fail failed: %v", err)
	}

	var state State
	var lastError *string
	err = pool.QueryRow(ctx, `SELECT state, last_error FROM miniqueue_jobs WHERE id = $1`, claimed.ID).Scan(&state, &lastError)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if state != StateFailed {
		t.Errorf("expected state 'failed', got %q", state)
	}
	if lastError == nil || *lastError != "SMTP connection refused" {
		t.Errorf("expected last_error 'SMTP connection refused', got %v", lastError)
	}
}

func TestRenewLease(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "emails", Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := store.Claim(ctx, "emails", "worker-1", 5*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	originalExpiry := *claimed.LeaseExpiresAt

	if err := store.RenewLease(ctx, claimed.ID, "worker-1", 60*time.Second); err != nil {
		t.Fatalf("renew lease failed: %v", err)
	}

	var newExpiry time.Time
	err = pool.QueryRow(ctx, `SELECT lease_expires_at FROM miniqueue_jobs WHERE id = $1`, claimed.ID).Scan(&newExpiry)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !newExpiry.After(originalExpiry) {
		t.Errorf("expected lease extended; original=%v, new=%v", originalExpiry, newExpiry)
	}
}

func TestRecoverExpiredLeases(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueOptions{
		Queue: "emails", Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, err = store.Claim(ctx, "emails", "worker-1", 1*time.Second)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	// Wait for lease to expire.
	time.Sleep(2 * time.Second)

	recovered, err := store.RecoverExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered, got %d", recovered)
	}

	// Should be claimable again.
	reclaimed, err := store.Claim(ctx, "emails", "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}
	if reclaimed.Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", reclaimed.Attempt)
	}
	if reclaimed.LeasedBy == nil || *reclaimed.LeasedBy != "worker-2" {
		t.Errorf("expected leased_by 'worker-2', got %v", reclaimed.LeasedBy)
	}
}

func TestConcurrentClaim_SKIPLOCKED(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	const numJobs = 20
	const numWorkers = 5

	for i := 0; i < numJobs; i++ {
		if _, err := store.Enqueue(ctx, EnqueueOptions{
			Queue: "concurrent", Payload: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	var claimedIDs sync.Map
	var wg sync.WaitGroup
	var totalClaimed atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%d", w)
		go func() {
			defer wg.Done()
			for {
				job, err := store.Claim(ctx, "concurrent", workerID, 30*time.Second)
				if errors.Is(err, ErrNoJobAvailable) {
					return
				}
				if err != nil {
					t.Errorf("claim error for %s: %v", workerID, err)
					return
				}
				if _, loaded := claimedIDs.LoadOrStore(job.ID, workerID); loaded {
					t.Errorf("job %d claimed by multiple workers!", job.ID)
				}
				totalClaimed.Add(1)
			}
		}()
	}

	wg.Wait()

	if totalClaimed.Load() != int64(numJobs) {
		t.Errorf("expected %d claims, got %d", numJobs, totalClaimed.Load())
	}
}

func TestRecordFailure_Retry(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "retry-test",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.Claim(ctx, "retry-test", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Record failure — should go back to available with future scheduled_at
	fixedBackoff := func(attempt int) time.Duration { return 2 * time.Second }
	err = store.RecordFailure(ctx, claimed.ID, "worker-1", "transient error", fixedBackoff)
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}

	// Verify state
	var state State
	var scheduledAt time.Time
	var lastError *string
	err = pool.QueryRow(ctx,
		"SELECT state, scheduled_at, last_error FROM miniqueue_jobs WHERE id = $1", job.ID,
	).Scan(&state, &scheduledAt, &lastError)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if state != StateAvailable {
		t.Errorf("expected state 'available', got %q", state)
	}
	if lastError == nil || *lastError != "transient error" {
		t.Errorf("expected last_error 'transient error', got %v", lastError)
	}
	// scheduled_at should be ~2s in the future
	if scheduledAt.Before(time.Now().Add(1 * time.Second)) {
		t.Errorf("expected scheduled_at ~2s in future, got %v", scheduledAt)
	}
}

func TestRecordFailure_DeadLetter(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	// Enqueue with max_attempts = 2
	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "dead-test",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Claim and fail twice to exhaust max_attempts
	for i := 0; i < 2; i++ {
		// Small sleep to ensure scheduled_at (set by Go's clock) is
		// in the past relative to Postgres's now() on the next claim.
		time.Sleep(50 * time.Millisecond)

		claimed, err := store.Claim(ctx, "dead-test", "worker-1", 30*time.Second)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}

		// Use zero backoff so the job is immediately claimable again
		err = store.RecordFailure(ctx, claimed.ID, "worker-1", "permanent error", func(int) time.Duration { return 0 })
		if err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}

	// After 2 attempts, job should be dead
	var state State
	var attempt int
	err = pool.QueryRow(ctx,
		"SELECT state, attempt FROM miniqueue_jobs WHERE id = $1", job.ID,
	).Scan(&state, &attempt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if state != StateDead {
		t.Errorf("expected state 'dead', got %q", state)
	}
	if attempt != 2 {
		t.Errorf("expected attempt 2, got %d", attempt)
	}
}

func TestRequeue(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:       "requeue-test",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Claim and fail to get to dead state
	claimed, err := store.Claim(ctx, "requeue-test", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	err = store.RecordFailure(ctx, claimed.ID, "worker-1", "fatal", func(int) time.Duration { return 0 })
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}

	// Verify it's dead
	var state State
	_ = pool.QueryRow(ctx, "SELECT state FROM miniqueue_jobs WHERE id = $1", job.ID).Scan(&state)
	if state != StateDead {
		t.Fatalf("expected dead, got %q", state)
	}

	// Requeue it
	err = store.Requeue(ctx, job.ID)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}

	// Verify it's available again
	_ = pool.QueryRow(ctx, "SELECT state FROM miniqueue_jobs WHERE id = $1", job.ID).Scan(&state)
	if state != StateAvailable {
		t.Errorf("expected available after requeue, got %q", state)
	}

	// Should be claimable
	reclaimed, err := store.Claim(ctx, "requeue-test", "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("reclaim after requeue: %v", err)
	}
	if reclaimed.ID != job.ID {
		t.Errorf("reclaimed wrong job: want %d, got %d", job.ID, reclaimed.ID)
	}
}

func TestRequeue_NonDeadJob(t *testing.T) {
	pool := testDB(t)
	store := NewStore(pool)
	ctx := context.Background()

	job, err := store.Enqueue(ctx, EnqueueOptions{
		Queue:   "requeue-fail-test",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Try to requeue an available (non-dead) job
	err = store.Requeue(ctx, job.ID)
	if err == nil {
		t.Error("expected error requeuing non-dead job")
	}
}