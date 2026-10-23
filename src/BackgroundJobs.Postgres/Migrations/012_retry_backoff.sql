-- Exponential retry backoff, stored per job so a retry is computed with the settings the job was enqueued with.
-- Each retry delay is retry_delay_ms * retry_backoff_multiplier^(counted attempts - 1); 1 (every existing row) is the
-- fixed delay, unchanged.
ALTER TABLE bj_jobs ADD COLUMN retry_backoff_multiplier double precision NOT NULL DEFAULT 1;
-- Cap on the backed-off delay, before jitter; NULL means no cap.
ALTER TABLE bj_jobs ADD COLUMN max_retry_delay_ms int;
