-- Jobs wait in a named queue and are claimed by priority (higher first), then FIFO. Existing jobs land in 'default' at 0.
ALTER TABLE bj_jobs ADD COLUMN queue text NOT NULL DEFAULT 'default';
ALTER TABLE bj_jobs ADD COLUMN priority int NOT NULL DEFAULT 0;

-- The claim reads the head of one queue in claim order.
DROP INDEX bj_jobs_ready;
CREATE INDEX bj_jobs_ready ON bj_jobs (queue, priority DESC, enqueued_at, enqueue_seq) WHERE state = 'enqueued';
