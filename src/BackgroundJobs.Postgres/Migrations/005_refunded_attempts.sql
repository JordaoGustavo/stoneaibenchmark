-- Attempts of the current run given back by a server stop; attempts - refunded_attempts counts against max_attempts.
ALTER TABLE bj_jobs ADD COLUMN refunded_attempts int NOT NULL DEFAULT 0;
