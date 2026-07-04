-- 001_create_jobs.sql
-- Core jobs table for miniqueue.
-- Design decisions documented inline — these are the talking points in an interview.

CREATE TABLE IF NOT EXISTS miniqueue_jobs (
    id              BIGSERIAL       PRIMARY KEY,
    queue           TEXT            NOT NULL,
    idempotency_key TEXT            UNIQUE,
    payload         JSONB           NOT NULL,
    state           TEXT            NOT NULL DEFAULT 'available',
    priority        SMALLINT        NOT NULL DEFAULT 0,
    attempt         INT             NOT NULL DEFAULT 0,
    max_attempts    INT             NOT NULL DEFAULT 5,
    scheduled_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ,
    leased_by       TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,

    CONSTRAINT valid_state CHECK (state IN ('available', 'running', 'completed', 'failed', 'dead'))
);

-- Partial index: only indexes available jobs, keeping the index small and fast.
-- The (queue, priority DESC, scheduled_at) ordering means higher-priority jobs
-- are claimed first, with FIFO within the same priority level.
CREATE INDEX IF NOT EXISTS idx_miniqueue_jobs_claim
    ON miniqueue_jobs (queue, priority DESC, scheduled_at)
    WHERE state = 'available';

-- Index for the lease reaper: finds running jobs whose lease has expired.
CREATE INDEX IF NOT EXISTS idx_miniqueue_jobs_expired_leases
    ON miniqueue_jobs (lease_expires_at)
    WHERE state = 'running' AND lease_expires_at IS NOT NULL;
