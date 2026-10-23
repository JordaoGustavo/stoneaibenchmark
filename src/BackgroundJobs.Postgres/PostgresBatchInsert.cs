using BackgroundJobs.Storage;
using Dapper;
using Npgsql;

namespace BackgroundJobs.Postgres;

/// <summary>
/// Inserts a batch of jobs on a given transaction: the store's own (<see cref="PostgresJobStore.InsertManyAsync"/>) or the
/// application's (<see cref="PostgresJobClientExtensions"/>). Nothing here commits, rolls back or wakes local loops: the
/// NOTIFY it sends is delivered on commit, to every node's listener (this one's included), and dropped on rollback.
/// </summary>
internal static class PostgresBatchInsert
{
    /// <summary>
    /// One statement inserts the jobs and their history rows from arrays (the parameter count does not grow with the
    /// batch). Rows go in batch order so the enqueue sequence numbers follow it: since PostgreSQL 9.6 volatile functions in
    /// the select list (nextval) are evaluated after ORDER BY. A row whose key an unfinished job holds is skipped by
    /// ON CONFLICT, which also covers a key repeated within the batch (the caller sends only its first job, anyway).
    /// </summary>
    private const string InsertSql = $"""
        WITH input AS (
            SELECT * FROM unnest(@ids::uuid[], @names::text[], @args::text[], @argsVersions::int[], @states::text[], @maxAttempts::int[],
                                 @retryDelays::int[], @retryJitters::int[], @runAts::timestamptz[], @hasRunAts::bool[], @createdAts::timestamptz[],
                                 @queues::text[], @priorities::int[], @keys::text[], @reasons::text[], @continueAfters::uuid[],
                                 @conditions::text[], @backoffMultipliers::float8[], @maxRetryDelays::int[])
                WITH ORDINALITY AS t(id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms, run_at, has_run_at,
                                     created_at, queue, priority, idempotency_key, reason, continue_after, continuation_condition,
                                     retry_backoff_multiplier, max_retry_delay_ms, ord)
        ), ins AS (
            INSERT INTO bj_jobs (id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                                 run_at, enqueued_at, enqueue_seq, created_at, updated_at, queue, priority, idempotency_key,
                                 continue_after, continuation_condition, not_before, retry_backoff_multiplier, max_retry_delay_ms)
            SELECT id, name, args::jsonb, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                   CASE WHEN state = 'scheduled' THEN run_at END,
                   CASE WHEN state = 'enqueued' THEN created_at END,
                   CASE WHEN state = 'enqueued' THEN nextval('bj_enqueue_seq') END,
                   created_at, created_at, queue, priority, idempotency_key,
                   continue_after, continuation_condition,
                   -- As in the single insert: a continuation keeps its RunAt to decide on release whether it is scheduled.
                   CASE WHEN continue_after IS NOT NULL AND has_run_at THEN run_at END,
                   retry_backoff_multiplier, max_retry_delay_ms
            FROM input ORDER BY ord
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND {PostgresJobStore.KeyHeld} DO NOTHING
            RETURNING id
        ), history AS (
            INSERT INTO bj_history (job_id, state, at, reason)
            SELECT input.id, input.state, input.created_at, input.reason FROM ins JOIN input USING (id) ORDER BY input.ord
        )
        SELECT id FROM ins
        """;

    /// <summary>
    /// What <see cref="IJobStore.InsertManyAsync"/> returns, written on <paramref name="tx"/>.
    /// </summary>
    /// <exception cref="JobNotFoundException">A continuation's parent does not exist (nothing of the batch is kept once the
    /// transaction rolls back).</exception>
    public static async Task<IReadOnlyList<Guid>> InsertAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<NewJob> jobs, CancellationToken ct)
    {
        var result = new Guid[jobs.Count];
        // A key repeated in the batch goes to the store once; its later jobs get whatever its first one got.
        var firstWithKey = new Dictionary<string, int>(StringComparer.Ordinal);
        var pending = new List<int>();
        for (var i = 0; i < jobs.Count; i++)
            if (jobs[i].IdempotencyKey is not { } key || firstWithKey.TryAdd(key, i)) pending.Add(i);

        // A continuation whose parent is an earlier job of the batch starts a new segment, so that the parent is written
        // (or found deduplicated) before its state is read under lock, exactly as with one InsertAsync per job.
        var entered = new HashSet<JobState>();
        var segment = new List<int>();
        var segmentIds = new HashSet<Guid>();
        foreach (var i in pending)
        {
            if (jobs[i].ContinueAfter is { } parent && segmentIds.Contains(parent))
            {
                await InsertSegmentAsync(conn, tx, jobs, segment, result, entered, ct);
                segment = [];
                segmentIds.Clear();
            }
            segment.Add(i);
            segmentIds.Add(jobs[i].Id);
        }
        if (segment.Count > 0) await InsertSegmentAsync(conn, tx, jobs, segment, result, entered, ct);

        for (var i = 0; i < jobs.Count; i++)
            if (jobs[i].IdempotencyKey is { } key && firstWithKey[key] != i) result[i] = result[firstWithKey[key]];
        foreach (var state in entered) await PostgresJobStore.NotifyAsync(conn, tx, state);
        return result;
    }

    private static async Task InsertSegmentAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<NewJob> jobs, List<int> pending, Guid[] result, HashSet<JobState> entered,
        CancellationToken ct)
    {
        var creations = await CreationsAsync(conn, tx, jobs, pending, ct);
        while (pending.Count > 0)
        {
            var batch = pending.Select(i => (Job: jobs[i], Creation: creations[i])).ToList();
            var inserted = (await conn.QueryAsync<Guid>(new CommandDefinition(InsertSql, Parameters(batch), tx, cancellationToken: ct))).ToHashSet();
            var conflicted = new List<int>();
            foreach (var i in pending)
            {
                if (inserted.Contains(jobs[i].Id)) { result[i] = jobs[i].Id; entered.Add(creations[i].To); }
                else conflicted.Add(i);
            }
            if (conflicted.Count == 0) break;

            // Read committed: this statement sees the holders the insert conflicted with, unless one finished meanwhile;
            // then its key is free and that job is retried (as InsertAsync does).
            var keys = conflicted.Select(i => jobs[i].IdempotencyKey!).ToArray();
            var holders = (await conn.QueryAsync<(string Key, Guid Id)>(new CommandDefinition(
                    $"SELECT idempotency_key, id FROM bj_jobs WHERE idempotency_key = ANY(@keys) AND {PostgresJobStore.KeyHeld}",
                    new { keys }, tx, cancellationToken: ct)))
                .ToDictionary(h => h.Key, h => h.Id, StringComparer.Ordinal);
            pending = [];
            foreach (var i in conflicted)
            {
                if (holders.TryGetValue(jobs[i].IdempotencyKey!, out var holder)) result[i] = holder;
                else pending.Add(i);
            }
        }
    }

    /// <summary>
    /// Each job's creation transition. Continuations read their parents' states under FOR SHARE, as
    /// <see cref="PostgresJobStore.InsertAsync"/> does: a parent finishing concurrently either committed before (its finished
    /// state decides the continuation now) or waits for this transaction and then releases the awaiting continuation.
    /// </summary>
    private static async Task<Dictionary<int, JobTransition>> CreationsAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<NewJob> jobs, List<int> indexes, CancellationToken ct)
    {
        var parents = indexes.Where(i => jobs[i].ContinueAfter != null).Select(i => jobs[i].ContinueAfter!.Value).Distinct().ToArray();
        var states = new Dictionary<Guid, JobState>();
        if (parents.Length > 0)
        {
            // Locked in id order so concurrent batches sharing parents cannot deadlock on each other.
            var rows = await conn.QueryAsync<(Guid Id, string State)>(new CommandDefinition(
                "SELECT id, state FROM bj_jobs WHERE id = ANY(@parents) ORDER BY id FOR SHARE", new { parents }, tx, cancellationToken: ct));
            foreach (var (id, state) in rows) states[id] = JobStateWire.ParseState(state);
        }
        var creations = new Dictionary<int, JobTransition>();
        foreach (var i in indexes)
        {
            var job = jobs[i];
            if (job.ContinueAfter is not { } parent) { creations[i] = job.Creation; continue; }
            if (!states.TryGetValue(parent, out var parentState)) throw new JobNotFoundException(parent);
            creations[i] = JobTransition.CreateContinuation(parentState, job.Condition, job.RunAt > job.Now);
        }
        return creations;
    }

    private static object Parameters(List<(NewJob Job, JobTransition Creation)> batch)
    {
        var jobs = batch.Select(b => b.Job).ToList();
        return new
        {
            ids = jobs.Select(j => j.Id).ToArray(),
            names = jobs.Select(j => j.Name).ToArray(),
            args = jobs.Select(j => j.ArgsJson).ToArray(),
            argsVersions = jobs.Select(j => j.ArgsVersion).ToArray(),
            states = batch.Select(b => b.Creation.To.ToWire()).ToArray(),
            maxAttempts = jobs.Select(j => j.MaxAttempts).ToArray(),
            retryDelays = jobs.Select(j => j.RetryDelayMs).ToArray(),
            retryJitters = jobs.Select(j => j.RetryJitterMs).ToArray(),
            // Read only for scheduled jobs and continuations with a RunAt (hasRunAts); no nulls, so Npgsql infers timestamptz[].
            runAts = jobs.Select(j => Utc(j.RunAt ?? j.Now)).ToArray(),
            hasRunAts = jobs.Select(j => j.RunAt != null).ToArray(),
            createdAts = jobs.Select(j => Utc(j.Now)).ToArray(),
            queues = jobs.Select(j => j.Queue).ToArray(),
            priorities = jobs.Select(j => j.Priority).ToArray(),
            keys = jobs.Select(j => j.IdempotencyKey).ToArray(),
            reasons = batch.Select(b => b.Creation.Reason).ToArray(),
            continueAfters = jobs.Select(j => j.ContinueAfter).ToArray(),
            conditions = jobs.Select(j => j.ContinueAfter == null ? null : j.Condition.ToWire()).ToArray(),
            backoffMultipliers = jobs.Select(j => j.RetryBackoffMultiplier).ToArray(),
            maxRetryDelays = jobs.Select(j => j.MaxRetryDelayMs).ToArray(),
        };
    }

    /// <summary>
    /// NewJob times are UTC (see NewJob); Npgsql writes timestamptz arrays only from UTC-kind values.
    /// </summary>
    private static DateTime Utc(DateTime t) => t.Kind == DateTimeKind.Utc ? t : DateTime.SpecifyKind(t, DateTimeKind.Utc);
}
