CREATE SEQUENCE bj_enqueue_seq;

CREATE TABLE bj_jobs (
    id               uuid PRIMARY KEY,           -- UUIDv7: sorts by creation time
    name             text        NOT NULL,
    args             jsonb       NOT NULL,
    state            text        NOT NULL,
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    retry_delay_ms   int         NOT NULL,
    retry_jitter_ms  int         NOT NULL,
    run_at           timestamptz NULL,           -- only while scheduled
    enqueued_at      timestamptz NULL,           -- last entry into enqueued (FIFO key)
    enqueue_seq      bigint      NULL,           -- FIFO tie-breaker
    run              int         NOT NULL DEFAULT 1, -- bumped on manual requeue; groups attempts
    version          int         NOT NULL DEFAULT 1, -- optimistic concurrency
    locked_by        text        NULL,
    lease_expires_at timestamptz NULL,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);

CREATE INDEX bj_jobs_ready ON bj_jobs (enqueued_at, enqueue_seq) WHERE state = 'enqueued';
CREATE INDEX bj_jobs_due ON bj_jobs (run_at) WHERE state = 'scheduled';
CREATE INDEX bj_jobs_processing ON bj_jobs (lease_expires_at) WHERE state = 'processing';

CREATE TABLE bj_attempts (
    id          uuid PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES bj_jobs (id),
    run         int         NOT NULL,
    attempt     int         NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NULL,
    outcome     text        NULL,
    error       text        NULL
);

CREATE INDEX bj_attempts_job ON bj_attempts (job_id, started_at);

CREATE TABLE bj_history (
    id     bigserial PRIMARY KEY,
    job_id uuid        NOT NULL REFERENCES bj_jobs (id),
    state  text        NOT NULL,
    at     timestamptz NOT NULL,
    reason text        NOT NULL
);

CREATE INDEX bj_history_job ON bj_history (job_id, id);
