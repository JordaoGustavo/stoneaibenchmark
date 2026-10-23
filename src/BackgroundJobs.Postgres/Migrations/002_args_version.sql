-- Jobs enqueued before args versioning were written with version 1 args.
ALTER TABLE bj_jobs ADD COLUMN args_version int NOT NULL DEFAULT 1;
