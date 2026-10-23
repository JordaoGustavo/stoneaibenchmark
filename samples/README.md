# Samples

Each sample is one `Program.cs` against PostgreSQL. Start the database first (see [RUNNING.md](../RUNNING.md)):

```sh
docker compose up -d
```

The connection string defaults to `Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs`; set `DATABASE_URL` to override it.
Each sample uses its own queue (`sample-*`). Nodes already skip job names they don't register; the separate queue only matters if another app on the same database registers the same name.

| Sample | Shows | Run |
|---|---|---|
| [WorkerExample](WorkerExample/Program.cs) | Minimal worker: register a job, enqueue one now and one scheduled with retry options. Runs until Ctrl+C. | `dotnet run --project samples/WorkerExample` |
| [AspNetApi](AspNetApi/Program.cs) | Web app with `MapBackgroundJobsApi()` (`/api/jobs`, `/api/recurring`, `/health`) and `MapBackgroundJobsDashboard()` (`/jobs`); `POST /signup?email=...` enqueues a job. Runs until Ctrl+C. | `dotnet run --project samples/AspNetApi` |
| [Recurring](Recurring/Program.cs) | Cron jobs: `AddRecurringJob` declared in code (every 2s, seconds field), `IRecurringJobClient.UpsertAsync` with a time zone, `TriggerAsync`, `RemoveAsync`. Exits after ~7s. | `dotnet run --project samples/Recurring` |
| [Idempotency](Idempotency/Program.cs) | Enqueueing twice with the same `JobOptions.IdempotencyKey` returns the same id; once the job finishes the key is free. | `dotnet run --project samples/Idempotency` |
| [Cancellation](Cancellation/Program.cs) | A long job honoring `context.CancellationToken`, stopped with `IJobClient.CancelAsync`; it ends `cancelled`. | `dotnet run --project samples/Cancellation` |
| [Concurrency](Concurrency/Program.cs) | Per-job `MaxConcurrency = 2` with 8 workers: six jobs, never more than two running at once, across the cluster. | `dotnet run --project samples/Concurrency` |
| [RetryBackoff](RetryBackoff/Program.cs) | Exponential retry backoff (`RetryBackoff.Exponential`, `RetryBackoffMultiplier`, `MaxRetryDelay`, plus the `Default*` options): a job fails three times, retried after ~0.5s, 1s, 2s. | `dotnet run --project samples/RetryBackoff` |
| [Continuations](Continuations/Program.cs) | `JobOptions.ContinueAfter`: a three-step chain waiting in `awaiting`, and a failing parent whose `OnSucceeded` child is cancelled while its `OnFinished` child runs. | `dotnet run --project samples/Continuations` |
| [Outbox](Outbox/Program.cs) | Transactional enqueue: an app table row and its jobs in one `NpgsqlTransaction` (`EnqueueManyAsync(tx, ...)` committed, `EnqueueAsync(tx, ...)` rolled back), plus a batch `EnqueueManyAsync` outside a transaction. Creates `sample_orders`. | `dotnet run --project samples/Outbox` |
