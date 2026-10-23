-- At most one unfinished job per idempotency key; a finished job no longer holds its key. States spelled out: the
-- complement of JobStateWire.Finished.
ALTER TABLE bj_jobs ADD COLUMN idempotency_key text NULL;

CREATE UNIQUE INDEX bj_jobs_idempotency_key ON bj_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL AND state IN ('scheduled', 'enqueued', 'processing');
