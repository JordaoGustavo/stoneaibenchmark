-- Claims count the processing jobs of each name that has a concurrency limit.
CREATE INDEX bj_jobs_processing_name ON bj_jobs (name) WHERE state = 'processing';
