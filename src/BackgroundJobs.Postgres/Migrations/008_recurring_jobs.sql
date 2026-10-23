-- Recurring definitions; the scheduler fires those whose next_run_at has passed.
CREATE TABLE bj_recurring (
    id              text PRIMARY KEY,
    job_name        text        NOT NULL,
    cron            text        NOT NULL,
    time_zone       text        NOT NULL,
    args            jsonb       NOT NULL,
    args_version    int         NOT NULL,
    max_attempts    int         NOT NULL,
    retry_delay_ms  int         NOT NULL,
    retry_jitter_ms int         NOT NULL,
    queue           text        NOT NULL,
    priority        int         NOT NULL,
    next_run_at     timestamptz NOT NULL,
    last_run_at     timestamptz NULL,
    last_job_id     uuid        NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE INDEX bj_recurring_due ON bj_recurring (next_run_at);
