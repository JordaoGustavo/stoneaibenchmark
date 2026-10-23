-- IJobClient.ListContinuationsAsync lists a job's continuations in every state, paged by id. bj_jobs_awaiting (011)
-- only covers awaiting rows, so released and finished continuations would need a full scan. Partial on
-- continue_after IS NOT NULL so ordinary jobs (most rows) stay out of it; (continue_after, id) serves the filter, the
-- id cursor and the ORDER BY in one range scan. bj_jobs_awaiting stays: it is smaller and on the release hot path.
CREATE INDEX bj_jobs_continuations ON bj_jobs (continue_after, id) WHERE continue_after IS NOT NULL;
