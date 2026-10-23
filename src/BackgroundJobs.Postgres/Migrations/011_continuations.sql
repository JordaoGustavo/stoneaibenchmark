-- Continuations (JobOptions.ContinueAfter): a job that waits in 'awaiting' for its parent to finish.
-- continue_after: the parent's id. No foreign key: a parent is purged by retention once finished, while its released
-- continuations may live on; continuations only await unfinished parents, which retention never purges.
-- continuation_condition: 'on_succeeded' or 'on_finished' (ContinuationCondition wire names).
-- not_before: the continuation's RunAt, kept while it awaits (run_at is only set while scheduled) to decide on release
-- whether it is scheduled or enqueued.
ALTER TABLE bj_jobs
    ADD COLUMN continue_after uuid NULL,
    ADD COLUMN continuation_condition text NULL,
    ADD COLUMN not_before timestamptz NULL;

-- A parent entering a finished state looks up its awaiting continuations in the same transaction.
CREATE INDEX bj_jobs_awaiting ON bj_jobs (continue_after) WHERE state = 'awaiting';

-- An awaiting job is unfinished, so it holds its idempotency key too: the partial unique index of 009 is recreated
-- with 'awaiting' in its predicate (the complement of JobStateWire.Finished, spelled out).
DROP INDEX bj_jobs_idempotency_key;
CREATE UNIQUE INDEX bj_jobs_idempotency_key ON bj_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL AND state IN ('awaiting', 'scheduled', 'enqueued', 'processing');
