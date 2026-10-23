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
                                 @retryDelays::int[], @retryJitters::int[], @runAts::timestamptz[], @createdAts::timestamptz[], @queues::text[],
                                 @priorities::int[], @keys::text[], @reasons::text[])
                WITH ORDINALITY AS t(id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms, run_at, created_at,
                                     queue, priority, idempotency_key, reason, ord)
        ), ins AS (
            INSERT INTO bj_jobs (id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                                 run_at, enqueued_at, enqueue_seq, created_at, updated_at, queue, priority, idempotency_key)
            SELECT id, name, args::jsonb, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                   CASE WHEN state = 'scheduled' THEN run_at END,
                   CASE WHEN state = 'enqueued' THEN created_at END,
                   CASE WHEN state = 'enqueued' THEN nextval('bj_enqueue_seq') END,
                   created_at, created_at, queue, priority, idempotency_key
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
    public static async Task<IReadOnlyList<Guid>> InsertAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<NewJob> jobs, CancellationToken ct)
    {
        var result = new Guid[jobs.Count];
        // A key repeated in the batch goes to the store once; its later jobs get whatever its first one got.
        var firstWithKey = new Dictionary<string, int>(StringComparer.Ordinal);
        var pending = new List<int>();
        for (var i = 0; i < jobs.Count; i++)
            if (jobs[i].IdempotencyKey is not { } key || firstWithKey.TryAdd(key, i)) pending.Add(i);

        var entered = new HashSet<JobState>();
        while (pending.Count > 0)
        {
            var batch = pending.Select(i => jobs[i]).ToList();
            var inserted = (await conn.QueryAsync<Guid>(new CommandDefinition(InsertSql, Parameters(batch), tx, cancellationToken: ct))).ToHashSet();
            var conflicted = new List<int>();
            foreach (var i in pending)
            {
                if (inserted.Contains(jobs[i].Id)) { result[i] = jobs[i].Id; entered.Add(jobs[i].Creation.To); }
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

        for (var i = 0; i < jobs.Count; i++)
            if (jobs[i].IdempotencyKey is { } key && firstWithKey[key] != i) result[i] = result[firstWithKey[key]];
        foreach (var state in entered) await PostgresJobStore.NotifyAsync(conn, tx, state);
        return result;
    }

    private static object Parameters(List<NewJob> jobs) => new
    {
        ids = jobs.Select(j => j.Id).ToArray(),
        names = jobs.Select(j => j.Name).ToArray(),
        args = jobs.Select(j => j.ArgsJson).ToArray(),
        argsVersions = jobs.Select(j => j.ArgsVersion).ToArray(),
        states = jobs.Select(j => j.Creation.To.ToWire()).ToArray(),
        maxAttempts = jobs.Select(j => j.MaxAttempts).ToArray(),
        retryDelays = jobs.Select(j => j.RetryDelayMs).ToArray(),
        retryJitters = jobs.Select(j => j.RetryJitterMs).ToArray(),
        // Read only for scheduled jobs, which have a RunAt; no nulls, so Npgsql infers timestamptz[] for the array.
        runAts = jobs.Select(j => Utc(j.RunAt ?? j.Now)).ToArray(),
        createdAts = jobs.Select(j => Utc(j.Now)).ToArray(),
        queues = jobs.Select(j => j.Queue).ToArray(),
        priorities = jobs.Select(j => j.Priority).ToArray(),
        keys = jobs.Select(j => j.IdempotencyKey).ToArray(),
        reasons = jobs.Select(j => j.Creation.Reason).ToArray(),
    };

    /// <summary>
    /// NewJob times are UTC (see NewJob); Npgsql writes timestamptz arrays only from UTC-kind values.
    /// </summary>
    private static DateTime Utc(DateTime t) => t.Kind == DateTimeKind.Utc ? t : DateTime.SpecifyKind(t, DateTimeKind.Utc);
}
