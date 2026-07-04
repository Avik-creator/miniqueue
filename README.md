# miniqueue

A Postgres-backed job queue with lease-based semantics, at-least-once delivery, and crash recovery — built to understand the failure modes that production systems like [River](https://github.com/riverqueue/river), Sidekiq, and BullMQ solve.

> **This is not a River clone.** This is a deliberate disassembly of the job queue pattern to understand *why* each component exists. Every design decision is documented below with its tradeoff.

**2,914 lines of Go. 28 tests against real Postgres. Zero mocks. Benchmarks and chaos testing included.**

---

## Delivery Guarantees: At-Least-Once, Not Exactly-Once

Let's kill the fantasy up front. **"Exactly-once" delivery is impossible in distributed systems** without distributed consensus (two-phase commit, Paxos, Raft). Here's why:

- A worker processes a job and calls `COMMIT` on the completion.
- Between the `COMMIT` and the `ACK` back to the coordinator, the network partitions or the process crashes.
- The coordinator doesn't know if the job was completed. It retries. Now the job runs twice.

This isn't a bug — it's a fundamental property of asynchronous systems (the [Two Generals Problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem)).

**miniqueue guarantees at-least-once delivery.** Every enqueued job will be processed *at least once*. To prevent double-execution, consumers provide an **idempotency key** — a unique identifier per logical operation. The storage layer uses `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` to ensure deduplication at the enqueue boundary.

```go
key := "order-payment-12345"
client.Enqueue(ctx, miniqueue.EnqueueOptions{
    Queue:          "payments",
    Payload:        payload,
    IdempotencyKey: &key,
})
```

If the producer retries the enqueue (e.g., after a network timeout), the job is only stored once. The consumer's handler must also be idempotent — this is a property of the business logic, not something the queue can enforce.

---

## Quick Start

### Prerequisites

- Go 1.23+
- PostgreSQL 9.5+ (for `FOR UPDATE SKIP LOCKED` support)

### Run the demo

```bash
# Create a database
createdb miniqueue_dev

# Run with demo jobs
DATABASE_URL="postgres://localhost/miniqueue_dev" go run ./cmd/miniqueue/
```

The demo enqueues 5 jobs with different priorities. Watch them get processed in priority order:

```
📝 enqueued: process_payment    (priority 10)  ← claimed 1st
📝 enqueued: generate_thumbnail (priority 5)   ← claimed 2nd
📝 enqueued: sync_inventory     (priority 3)   ← claimed 3rd
📝 enqueued: send_welcome_email (priority 1)   ← claimed 4th
📝 enqueued: send_notification  (priority 1)   ← claimed 5th (FIFO within same priority)
```

### Use as a library

```go
package main

import (
    "context"
    "github.com/jackc/pgx/v5/pgxpool"
    miniqueue "avikmukherjee.com/miniqueue"
)

func main() {
    pool, _ := pgxpool.New(context.Background(), "postgres://localhost/mydb")
    defer pool.Close()

    miniqueue.RunMigrations(context.Background(), pool, "migrations")
    store := miniqueue.NewStore(pool)

    handler := miniqueue.HandlerFunc(func(ctx context.Context, job *miniqueue.Job) error {
        // Your business logic here
        return nil
    })

    worker := miniqueue.NewWorker(store, handler, miniqueue.WorkerConfig{
        Queue:    "emails",
        WorkerID: "worker-1",
    })

    reaper := miniqueue.NewReaper(store, miniqueue.ReaperConfig{})
    go reaper.Start(ctx)

    worker.Start(ctx) // blocks until shutdown
}
```

### Run the tests

```bash
createdb miniqueue_test
TEST_DATABASE_URL="postgres://localhost/miniqueue_test" go test -v -race ./...
```

---

## Architecture

### Storage Layer (`store.go`)

The storage layer owns all SQL. The single most important query is the **claim query**:

```sql
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
RETURNING *;
```

**Why `FOR UPDATE SKIP LOCKED`?**

- `FOR UPDATE` takes a row-level lock so no other transaction can claim the same job.
- `SKIP LOCKED` skips rows already locked by other transactions instead of blocking. This is critical for throughput — without it, workers block on each other or get serialization errors.
- We chose `SKIP LOCKED` over `NOWAIT` because `NOWAIT` throws an error on lock conflict, requiring error handling per poll. `SKIP LOCKED` just moves on.

**Why not `LISTEN/NOTIFY`?** Polling is our v1 tradeoff for simplicity and correctness. `LISTEN/NOTIFY` is a latency optimization (sub-millisecond vs poll interval), not a correctness requirement. It's a planned Phase 4 addition.

### Lease Model

Workers acquire a **time-bounded lease** on each job. This is the mechanism that makes crash recovery possible:

```
t=0s     Worker claims job. Lease expires at t=30s.
         ┌─ processJob goroutine: running handler
         └─ heartbeat goroutine: renews lease every 10s

t=10s    Heartbeat: RenewLease → success. Lease now expires at t=40s.
t=20s    Heartbeat: RenewLease → success. Lease now expires at t=50s.
t=22s    Handler returns nil → Complete() → state='completed'
```

If the worker crashes:

```
t=0s     Worker claims job. Lease expires at t=30s.
t=15s    Worker crashes (SIGKILL, OOM, power loss).
         Job stuck: state='running', lease_expires_at=t=30s

t=30s    Lease expires.
t=35s    Reaper scans, finds expired lease, resets to 'available'.
t=35.5s  Another worker claims the job as attempt=2.
```

**Worst-case recovery time = LeaseDuration + ReaperInterval** (default: 35 seconds).

### Retry & Dead-Letter (`retry.go`, `store.go`)

Failed jobs follow this lifecycle:

```
Handler returns error
        │
        ▼
  RecordFailure()
        │
        ├── attempt < max_attempts
        │       state → 'available'
        │       scheduled_at → now() + backoff(attempt)
        │       Worker picks it up again after the delay
        │
        └── attempt >= max_attempts
                state → 'dead'
                Manual Requeue() needed to retry
```

The backoff uses **exponential delay with full jitter** (from the [AWS Architecture Blog, 2015](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)):

| Attempt | Max Delay |
|---|---|
| 1 | 0–2 seconds |
| 2 | 0–4 seconds |
| 3 | 0–8 seconds |
| 4 | 0–16 seconds |
| 5+ | 0–30 minutes (capped) |

Jitter prevents thundering herds when many jobs fail simultaneously.

### Graceful Shutdown

When the worker receives a shutdown signal (SIGINT/SIGTERM):

1. The claim loop stops — no new jobs are claimed
2. In-flight handlers continue until they return or `ShutdownTimeout` elapses (default: 10s)
3. `Start()` returns
4. Any abandoned jobs are recovered by the reaper when their lease expires

---

## Project Structure

```
miniqueue/
├── job.go                    # Job struct, State enum, EnqueueOptions
├── store.go                  # All SQL: Enqueue, Claim, Complete, Fail,
│                             #   RenewLease, RecoverExpiredLeases,
│                             #   RecordFailure, Requeue
├── client.go                 # High-level Client API, RunMigrations
├── handler.go                # Handler interface, HandlerFunc adapter
├── worker.go                 # Claim loop, heartbeat, graceful shutdown
├── reaper.go                 # Background crash recovery goroutine
├── retry.go                  # DefaultBackoff (exponential + full jitter)
├── notify.go                 # LISTEN/NOTIFY support (optional push-based wake)
├── store_test.go             # 17 integration tests (storage layer)
├── worker_test.go            # 8 integration tests (runtime)
├── retry_test.go             # 2 unit tests (backoff math)
├── bench_test.go             # Performance benchmarks (throughput, latency)
├── chaos_test.go             # Chaos test (random worker kills, zero job loss)
├── migrations/
│   └── 001_create_jobs.sql   # Schema + partial indexes
└── cmd/miniqueue/
    └── main.go               # Runnable binary with demo jobs
```

---

## Test Suite

**28 tests, all passing under Go's race detector against real Postgres.**

### Storage Layer Tests (`store_test.go`)

| Test | What it proves |
|---|---|
| `TestEnqueue_Simple` | Basic insert, default values (state=available, attempt=0) |
| `TestEnqueue_IdempotencyKey_NoDuplicates` | `ON CONFLICT DO NOTHING` — same key returns same job ID |
| `TestEnqueue_RequiresQueueName` | Validation rejects empty queue name |
| `TestClaim_Basic` | `FOR UPDATE SKIP LOCKED` sets state=running, lease, attempt=1 |
| `TestClaim_EmptyQueue` | Returns `ErrNoJobAvailable` cleanly |
| `TestClaim_PriorityOrdering` | Higher priority job claimed first despite being inserted second |
| `TestClaim_ScheduledInFuture` | Future-scheduled jobs invisible to claim until time arrives |
| `TestComplete` | Sets state=completed, clears lease fields |
| `TestComplete_WrongWorker` | Returns `ErrJobNotLeased` when wrong worker tries to complete |
| `TestFail` | Sets state=failed, records last_error |
| `TestRenewLease` | Extends lease_expires_at beyond original value |
| `TestRecoverExpiredLeases` | Expired lease → available, reclaimable by another worker |
| `TestConcurrentClaim_SKIPLOCKED` | **5 goroutines, 20 jobs, zero duplicates** |
| `TestRecordFailure_Retry` | attempt < max → available with future scheduled_at |
| `TestRecordFailure_DeadLetter` | attempt ≥ max → dead |
| `TestRequeue` | Dead → available, claimable again |
| `TestRequeue_NonDeadJob` | Rejects requeue on non-dead job |

### Runtime Tests (`worker_test.go`)

| Test | Duration | What it proves |
|---|---|---|
| `TestWorker_ProcessesJob` | 0.2s | Basic happy path: enqueue → claim → handler → complete |
| `TestWorker_HeartbeatKeepsLeaseAlive` | **6.2s** | Handler sleeps 6s with a 2s lease. Heartbeat renews every 500ms. Job completes — lease never expired. |
| `TestWorker_FailedJobRecordsError` | 0.3s | Handler error → dead-lettered with error recorded |
| `TestWorker_GracefulShutdown` | **2.1s** | Context cancelled mid-flight. Worker waits 2s for handler to finish. Job completed, not abandoned. |
| `TestWorker_ReaperRecoversKilledWorker` | **1.3s** | **Worker A crashes → reaper recovers → Worker B picks up as attempt=2 → completes. Zero job loss.** |
| `TestWorker_ConcurrentWorkers` | 0.6s | 3 workers, 30 jobs, zero duplicates. All 30 completed. |
| `TestWorker_RetryAndDeadLetter` | 0.4s | Handler always fails → 3 attempts → dead-lettered |
| `TestWorker_EventualSuccessAfterRetry` | 0.4s | Handler fails twice, succeeds on 3rd attempt |

### Unit Tests (`retry_test.go`)

| Test | What it proves |
|---|---|
| `TestDefaultBackoff` | Backoff bounded per attempt, capped at 30 minutes |
| `TestDefaultBackoff_Jitter` | 100 calls produce ≥10 distinct delay values (jitter works) |

### Benchmarks (`bench_test.go`)

Run benchmarks with:
```bash
TEST_DATABASE_URL="postgres://localhost/miniqueue_test" go test -bench=. -benchmem -run=^$ ./...
```

### Chaos Test (`chaos_test.go`)

| Test | What it proves |
|---|---|
| `TestChaos_RandomWorkerKills` | **4 workers, 50 jobs, random kills every 1-3s. All 50 completed. Zero duplicates. Zero losses.** |

---

## Performance

Benchmarks run on Apple M1 with PostgreSQL 16 (Docker). All numbers from `bench_test.go`.

### Throughput

| Operation | Workers | Time/op | Ops/sec | Memory |
|-----------|---------|---------|---------|--------|
| Enqueue | 1 | 1.2ms | ~816 | 1.7KB / 31 allocs |
| Claim | 1 | 1.1ms | ~913 | 1.7KB / 33 allocs |
| Claim | 2 | 1.0ms | ~1029 | 1.7KB / 33 allocs |
| Claim | 4 | 0.65ms | ~1530 | 1.8KB / 33 allocs |
| Claim | 8 | 0.48ms | **~2099** | 2.0KB / 34 allocs |
| Enqueue + NOTIFY | 1 | 1.6ms | ~625 | 1.8KB / 35 allocs |

**Key observations:**

1. **Excellent scaling with SKIP LOCKED**: Going from 1 to 8 workers more than doubles throughput (913 → 2099 ops/sec), demonstrating minimal contention.
2. **Predictable memory**: ~1.7KB per operation with ~33 allocations shows consistent behavior with no memory leaks.
3. **LISTEN/NOTIFY overhead**: The `pg_notify()` call adds ~27% overhead (1.2ms → 1.6ms), but this is a one-time cost per enqueue. The latency benefit for workers is substantial (sub-millisecond wake vs polling).
4. **Claim latency**: Average claim latency is ~1.5ms, which includes the `FOR UPDATE SKIP LOCKED` row lock acquisition.

### Chaos Testing Results

The `TestChaos_RandomWorkerKills` test proves correctness under failure:

```
Final state: completed=50, failed=0, dead=0, available=0, running=0
Handler called 50 times for 50 jobs
```

4 workers process 50 jobs while being randomly killed every 1-3 seconds (no graceful shutdown, no Complete/Fail call). The reaper recovers expired leases. **Zero job loss, zero duplicates.**

---

## Where This Diverges from River (and Why)

| Feature | River | miniqueue | Why the difference |
|---|---|---|---|
| **Delivery guarantee** | At-least-once | At-least-once | Same. Both are honest about this. |
| **Claim mechanism** | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | Same core pattern. |
| **Notifications** | `LISTEN/NOTIFY` (push) | Polling (pull) | Polling is simpler and correct. `LISTEN/NOTIFY` is a latency optimization, not a correctness requirement. |
| **Migrations** | Dedicated migration system | Flat SQL files | Good enough for a single-purpose library. |
| **Plugin architecture** | Full plugin system | None | Scope. Plugins solve extensibility for a library used by many teams. |
| **Retry backoff** | Configurable per-job | Global `BackoffFunc` | Simpler API surface. |
| **Multi-queue** | Per-worker queue list | Single queue per worker | Simpler claim loop. |
| **Periodic jobs** | Built-in cron-like scheduling | Not implemented | Out of scope. `ScheduledAt` covers the basic case. |
| **Observability** | Structured logging + metrics | `log/slog` only | Minimal dependency surface. |

**The point isn't to match River's feature set.** The point is to understand *why* each feature exists by implementing the core and feeling where the pain points are.

---

## Design Decisions Log

| Decision | Choice | Rationale |
|---|---|---|
| Database | PostgreSQL | `FOR UPDATE SKIP LOCKED` is the correct primitive for concurrent job claiming. Redis lacks row-level locking. |
| Driver | `pgx/v5` | Best Postgres support in Go. Connection pooling, prepared statements, native type mapping. |
| Claim concurrency | `FOR UPDATE SKIP LOCKED` | Avoids blocking and error handling per-poll. Workers skip locked rows and move to the next. |
| Lease model | Time-bounded with heartbeat | Crash recovery without external coordination. Worst-case recovery = LeaseDuration + ReaperInterval. |
| Heartbeat interval | LeaseDuration / 3 | 3 chances to renew before expiry. If one fails (transient DB error), 2 more attempts remain. |
| Retry backoff | Exponential + full jitter | Prevents thundering herds. Capped at 30 minutes to avoid unbounded growth. |
| Dead-letter | State transition, not separate table | Simpler queries. Dead jobs are just `state='dead'` rows with a `Requeue()` method. |
| Idempotency | `UNIQUE` constraint on `idempotency_key` | Database-enforced deduplication. No application-level coordination needed. |
| Shutdown | Drain with timeout | In-flight jobs get a chance to complete. Abandoned jobs are recovered by the reaper. |
| Polling interval | 1 second (default) | Good enough for most workloads. Sub-second latency requires `LISTEN/NOTIFY`. |
| Partial indexes | `WHERE state = 'available'` | Keeps the claim index small as the table grows with completed/failed jobs. |

---

## Schema

```sql
CREATE TABLE miniqueue_jobs (
    id               BIGSERIAL       PRIMARY KEY,
    queue            TEXT            NOT NULL,
    idempotency_key  TEXT            UNIQUE,
    payload          JSONB           NOT NULL,
    state            TEXT            NOT NULL DEFAULT 'available',
    priority         SMALLINT        NOT NULL DEFAULT 0,
    attempt          INT             NOT NULL DEFAULT 0,
    max_attempts     INT             NOT NULL DEFAULT 5,
    scheduled_at     TIMESTAMPTZ     NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ,
    leased_by        TEXT,
    last_error       TEXT,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,

    CONSTRAINT valid_state CHECK (
        state IN ('available', 'running', 'completed', 'failed', 'dead')
    )
);

-- Partial index: only indexes available jobs.
CREATE INDEX idx_miniqueue_jobs_claim
    ON miniqueue_jobs (queue, priority DESC, scheduled_at)
    WHERE state = 'available';

-- Partial index for the reaper: finds expired leases.
CREATE INDEX idx_miniqueue_jobs_expired_leases
    ON miniqueue_jobs (lease_expires_at)
    WHERE state = 'running' AND lease_expires_at IS NOT NULL;
```

---

## License

MIT