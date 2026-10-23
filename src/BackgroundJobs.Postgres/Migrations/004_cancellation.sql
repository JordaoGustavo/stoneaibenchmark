-- Set while a user's cancel of a processing job waits for its node to act on it.
ALTER TABLE bj_jobs ADD COLUMN cancel_requested_at timestamptz NULL;
-- Cancelled is a finished state too.
DROP INDEX bj_jobs_finished;
CREATE INDEX bj_jobs_finished ON bj_jobs (updated_at) WHERE state IN ('succeeded', 'failed', 'deleted', 'cancelled');
