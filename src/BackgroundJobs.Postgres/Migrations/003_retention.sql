-- Finds finished jobs past retention without scanning live ones.
CREATE INDEX bj_jobs_finished ON bj_jobs (updated_at) WHERE state IN ('succeeded', 'failed', 'deleted');
