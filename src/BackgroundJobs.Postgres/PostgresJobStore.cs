using System.Text.Json;
using Dapper;
using BackgroundJobs.Storage;
using Npgsql;

namespace BackgroundJobs.Postgres;

internal sealed class PostgresJobStore(NpgsqlDataSource db) : IJobStore
{
    static PostgresJobStore() => DefaultTypeMap.MatchNamesWithUnderscores = true;

    public async Task MigrateAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync("SELECT pg_advisory_xact_lock(7212300417)", transaction: tx);
        await conn.ExecuteAsync("CREATE TABLE IF NOT EXISTS bj_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())", transaction: tx);
        var applied = (await conn.QueryAsync<string>("SELECT name FROM bj_migrations", transaction: tx)).ToHashSet();

        var assembly = typeof(PostgresJobStore).Assembly;
        foreach (var resource in assembly.GetManifestResourceNames().Where(n => n.EndsWith(".sql")).Order())
        {
            var name = resource.Split('.')[^2];
            if (applied.Contains(name)) continue;
            using var reader = new StreamReader(assembly.GetManifestResourceStream(resource)!);
            await conn.ExecuteAsync(await reader.ReadToEndAsync(ct), transaction: tx);
            await conn.ExecuteAsync("INSERT INTO bj_migrations (name) VALUES (@name)", new { name }, tx);
        }
        await tx.CommitAsync(ct);
    }

    public async Task InsertAsync(NewJob job, CancellationToken ct)
    {
        var creation = job.Creation;
        var scheduled = creation.To == JobState.Scheduled;
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync("""
            INSERT INTO bj_jobs (id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                                 run_at, enqueued_at, enqueue_seq, created_at, updated_at)
            VALUES (@Id, @Name, @ArgsJson::jsonb, @ArgsVersion, @State, @MaxAttempts, @RetryDelayMs, @RetryJitterMs,
                    @RunAt, @EnqueuedAt, CASE WHEN @Scheduled THEN NULL ELSE nextval('bj_enqueue_seq') END, @Now, @Now)
            """,
            new
            {
                job.Id, job.Name, job.ArgsJson, job.ArgsVersion, State = creation.To.ToWire(), job.MaxAttempts, job.RetryDelayMs, job.RetryJitterMs,
                RunAt = scheduled ? job.RunAt : null, EnqueuedAt = scheduled ? (DateTime?)null : job.Now, Scheduled = scheduled, job.Now,
            }, tx);
        await InsertHistoryAsync(conn, tx, job.Id, creation.To, job.Now, creation.Reason);
        await NotifyAsync(conn, tx, creation.To);
        await tx.CommitAsync(ct);
    }

    public async Task<Job?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<JobRow>("SELECT *, args::text AS args_json FROM bj_jobs WHERE id = @id", new { id });
        if (row == null) return null;
        var history = await conn.QueryAsync<HistoryRow>("SELECT state, at, reason FROM bj_history WHERE job_id = @id ORDER BY id", new { id });
        var attempts = await conn.QueryAsync<AttemptRow>(
            "SELECT attempt, started_at, finished_at, outcome, error FROM bj_attempts WHERE job_id = @id ORDER BY started_at, id", new { id });

        var state = JobStateWire.ParseState(row.State);
        return new Job(
            row.Id, row.Name, JsonDocument.Parse(row.ArgsJson).RootElement.Clone(), row.ArgsVersion, state, row.Attempts, row.MaxAttempts,
            TimeSpan.FromMilliseconds(row.RetryDelayMs), state == JobState.Scheduled ? row.RunAt : null,
            row.CreatedAt, row.UpdatedAt,
            history.Select(h => new JobHistoryEntry(JobStateWire.ParseState(h.State), h.At, h.Reason)).ToList(),
            attempts.Select(a => new JobAttempt(a.Attempt, a.StartedAt, a.FinishedAt,
                a.Outcome == null ? null : JobStateWire.ParseOutcome(a.Outcome), a.Error)).ToList());
    }

    public async Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SummaryRow>("""
            SELECT id, name, state, attempts, run_at, created_at, updated_at FROM bj_jobs
            WHERE @after::uuid IS NULL OR id > @after::uuid
            ORDER BY id LIMIT @limit
            """, new { after, limit });
        return rows.Select(r => new JobSummary(r.Id, r.Name, JobStateWire.ParseState(r.State), r.Attempts, r.RunAt, r.CreatedAt, r.UpdatedAt)).ToList();
    }

    public async Task<DateTime?> NextRunAtAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<DateTime?>("SELECT min(run_at) FROM bj_jobs WHERE state = 'scheduled'");
    }

    public async Task<int> PromoteDueAsync(DateTime now, CancellationToken ct)
    {
        var t = JobTransition.Promote;
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var promoted = await conn.ExecuteAsync("""
            WITH due AS (
                SELECT id FROM bj_jobs WHERE state = ANY(@from) AND run_at <= @now
                ORDER BY run_at, id FOR UPDATE SKIP LOCKED
            ), promoted AS (
                UPDATE bj_jobs j SET state = @to, run_at = NULL, enqueued_at = @now,
                       enqueue_seq = nextval('bj_enqueue_seq'), version = j.version + 1, updated_at = @now
                FROM due WHERE j.id = due.id
                RETURNING j.id
            )
            INSERT INTO bj_history (job_id, state, at, reason) SELECT id, @to, @now, @reason FROM promoted
            """, new { now, from = Wire(t.From), to = t.To.ToWire(), reason = t.Reason }, tx);
        if (promoted > 0) await NotifyAsync(conn, tx, t.To);
        await tx.CommitAsync(ct);
        return promoted;
    }

    public async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(string nodeId, DateTime now, DateTime leaseUntil, int limit, IReadOnlyCollection<string> excludedNames, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        // One statement = one transaction: claim, attempts++, open attempts, history.
        var rows = await conn.QueryAsync<ClaimedJob>("""
            WITH locked AS (
                SELECT id, enqueued_at, enqueue_seq FROM bj_jobs
                WHERE state = ANY(@from) AND NOT (name = ANY(@excluded))
                ORDER BY enqueued_at, enqueue_seq
                LIMIT @limit FOR UPDATE SKIP LOCKED
            ), next AS (
                SELECT id, (@attemptIds::uuid[])[row_number() OVER (ORDER BY enqueued_at, enqueue_seq)] AS attempt_id FROM locked
            ), claimed AS (
                UPDATE bj_jobs j SET state = @to, attempts = j.attempts + 1, version = j.version + 1,
                       updated_at = @now, locked_by = @nodeId, lease_expires_at = @leaseUntil
                FROM next WHERE j.id = next.id
                RETURNING j.*, next.attempt_id
            ), attempt AS (
                INSERT INTO bj_attempts (id, job_id, run, attempt, started_at)
                SELECT attempt_id, id, run, attempts, @now FROM claimed
            ), history AS (
                INSERT INTO bj_history (job_id, state, at, reason)
                SELECT id, @to, @now, @reason FROM claimed
            )
            SELECT id, name, args::text AS args_json, args_version, attempts AS attempt, max_attempts, retry_delay_ms, retry_jitter_ms,
                   version, attempt_id, run
            FROM claimed
            ORDER BY enqueued_at, enqueue_seq
            """,
            new { nodeId, now, leaseUntil, limit, attemptIds = Enumerable.Range(0, limit).Select(_ => Guid.CreateVersion7()).ToArray(), excluded = excludedNames.ToArray(),
                from = Wire(JobTransition.Claim.From), to = JobTransition.Claim.To.ToWire(), reason = JobTransition.Claim.Reason });
        return rows.AsList();
    }

    public async Task<QueueStats> QueueStatsAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<QueueStats>(
            "SELECT count(*) AS ready, min(enqueued_at) AS oldest_ready_at FROM bj_jobs WHERE state = 'enqueued'");
    }

    public async Task<bool> FinishAsync(
        ClaimedJob job, string nodeId, JobTransition transition, AttemptOutcome outcome, string? error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        // Locked so a concurrent CancelAsync either lands before (and is seen here) or after (and finds the job finished).
        var cancelRequested = await conn.ExecuteScalarAsync<bool>(
            "SELECT cancel_requested_at IS NOT NULL FROM bj_jobs WHERE id = @id FOR UPDATE", new { id = job.Id }, tx);
        if (cancelRequested && transition != JobTransition.Succeed)
            (transition, outcome) = (JobTransition.Cancel, AttemptOutcome.Cancelled);
        if (!await TransitionAsync(conn, tx, job.Id, job.Version, transition, finishedAt, retryAt, nodeId)) return false;
        await conn.ExecuteAsync(
            "UPDATE bj_attempts SET finished_at = @at, outcome = @outcome, error = @error WHERE id = @id",
            new { at = finishedAt, outcome = outcome.ToWire(), error, id = job.AttemptId }, tx);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<IReadOnlyCollection<Guid>> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct)
    {
        if (ids.Count == 0) return [];
        await using var conn = await db.OpenConnectionAsync(ct);
        var renewed = await conn.QueryAsync<(Guid Id, bool CancelRequested)>("""
            UPDATE bj_jobs SET lease_expires_at = @leaseUntil WHERE id = ANY(@ids) AND locked_by = @nodeId AND state = 'processing'
            RETURNING id, cancel_requested_at IS NOT NULL
            """, new { leaseUntil, ids = ids.ToArray(), nodeId });
        return renewed.Where(r => r.CancelRequested).Select(r => r.Id).ToList();
    }

    public async Task<bool> CancelAsync(Guid id, DateTime now, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var current = await conn.QuerySingleOrDefaultAsync<(string State, int Version)?>(
            "SELECT state, version FROM bj_jobs WHERE id = @id FOR UPDATE", new { id }, tx);
        if (current is null) throw new JobNotFoundException(id);
        var requested = current.Value.State == JobState.Processing.ToWire();
        if (requested)
            await conn.ExecuteAsync("UPDATE bj_jobs SET cancel_requested_at = coalesce(cancel_requested_at, @now) WHERE id = @id", new { id, now }, tx);
        else if (!await TransitionAsync(conn, tx, id, current.Value.Version, JobTransition.Cancel, now))
            throw new InvalidJobTransitionException(id, JobStateWire.ParseState(current.Value.State), JobTransition.Cancel.Operation);
        await tx.CommitAsync(ct);
        return requested;
    }

    public async Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var stale = (await conn.QueryAsync<StaleRow>("""
            SELECT id, version, run, attempts, max_attempts, cancel_requested_at IS NOT NULL AS cancel_requested FROM bj_jobs
            WHERE state = 'processing' AND (lease_expires_at < @now OR locked_by = @ownNodeId)
            FOR UPDATE SKIP LOCKED
            """, new { now, ownNodeId }, tx)).ToList();

        foreach (var job in stale)
        {
            await conn.ExecuteAsync(
                "UPDATE bj_attempts SET finished_at = @now, outcome = @outcome WHERE job_id = @id AND run = @run AND finished_at IS NULL",
                new { now, job.Id, job.Run, outcome = (job.CancelRequested ? AttemptOutcome.Cancelled : AttemptOutcome.Interrupted).ToWire() }, tx);
            var transition = job.CancelRequested ? JobTransition.Cancel : JobTransition.Recover(job.Attempts, job.MaxAttempts);
            await TransitionAsync(conn, tx, job.Id, job.Version, transition, now);
        }
        await tx.CommitAsync(ct);
        return stale.Count;
    }

    public async Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var current = await conn.QuerySingleOrDefaultAsync<(string State, int Version)?>(
            "SELECT state, version FROM bj_jobs WHERE id = @id", new { id }, tx);
        if (current is null) throw new JobNotFoundException(id);
        if (!await TransitionAsync(conn, tx, id, current.Value.Version, transition, now))
            throw new InvalidJobTransitionException(id, JobStateWire.ParseState(current.Value.State), transition.Operation);
        await tx.CommitAsync(ct);
    }

    /// The shared mechanics of IJobStore for one job: guarded by version, source state and owner; always writes history.
    private static async Task<bool> TransitionAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, int version, JobTransition t, DateTime now, DateTime? runAt = null, string? lockedBy = null)
    {
        var changed = await conn.ExecuteAsync("""
            UPDATE bj_jobs SET
                state = @to, version = version + 1, updated_at = @now,
                run_at = CASE WHEN @to = 'scheduled' THEN @runAt::timestamptz END,
                enqueued_at = CASE WHEN @to = 'enqueued' THEN @now ELSE enqueued_at END,
                enqueue_seq = CASE WHEN @to = 'enqueued' THEN nextval('bj_enqueue_seq') ELSE enqueue_seq END,
                attempts = CASE WHEN @resetAttempts THEN 0 WHEN @refundAttempt THEN attempts - 1 ELSE attempts END,
                run = CASE WHEN @resetAttempts THEN run + 1 ELSE run END,
                locked_by = NULL, lease_expires_at = NULL, cancel_requested_at = NULL
            WHERE id = @id AND version = @version AND state = ANY(@from)
              AND (@lockedBy::text IS NULL OR locked_by = @lockedBy::text)
            """,
            new { id, version, from = Wire(t.From), to = t.To.ToWire(), now, runAt, resetAttempts = t.ResetAttempts, refundAttempt = t.RefundAttempt, lockedBy },
            tx);
        if (changed == 0) return false;
        await InsertHistoryAsync(conn, tx, id, t.To, now, t.Reason);
        await NotifyAsync(conn, tx, t.To);
        return true;
    }

    public async Task<int> PurgeFinishedAsync(DateTime finishedBefore, int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        // States spelled out (= JobStateWire.Finished) so the bj_jobs_finished partial index applies.
        // SKIP LOCKED: a job being requeued right now is left alone, and concurrent sweeps on other nodes split the work.
        return await conn.ExecuteScalarAsync<int>("""
            WITH doomed AS (
                SELECT id FROM bj_jobs WHERE state IN ('succeeded', 'failed', 'deleted', 'cancelled') AND updated_at < @finishedBefore
                ORDER BY updated_at LIMIT @limit FOR UPDATE SKIP LOCKED
            ), history AS (
                DELETE FROM bj_history WHERE job_id IN (SELECT id FROM doomed)
            ), attempts AS (
                DELETE FROM bj_attempts WHERE job_id IN (SELECT id FROM doomed)
            ), jobs AS (
                DELETE FROM bj_jobs WHERE id IN (SELECT id FROM doomed) RETURNING 1
            )
            SELECT count(*)::int FROM jobs
            """, new { finishedBefore, limit });
    }

    private static string[] Wire(IEnumerable<JobState> states) => states.Select(s => s.ToWire()).ToArray();

    /// Delivered on commit; Postgres folds identical notifications within a transaction.
    private static Task NotifyAsync(NpgsqlConnection conn, NpgsqlTransaction tx, JobState entered) =>
        entered is JobState.Enqueued or JobState.Scheduled
            ? conn.ExecuteAsync("SELECT pg_notify(@channel, @payload)", new { channel = JobNotifications.Channel, payload = entered.ToWire() }, tx)
            : Task.CompletedTask;

    private static Task InsertHistoryAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, JobState state, DateTime at, string reason) =>
        conn.ExecuteAsync("INSERT INTO bj_history (job_id, state, at, reason) VALUES (@id, @state, @at, @reason)",
            new { id, state = state.ToWire(), at, reason }, tx);

    private sealed class JobRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public string ArgsJson { get; set; } = "";
        public int ArgsVersion { get; set; }
        public string State { get; set; } = "";
        public int Attempts { get; set; }
        public int MaxAttempts { get; set; }
        public int RetryDelayMs { get; set; }
        public DateTime? RunAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }

    private sealed class HistoryRow
    {
        public string State { get; set; } = "";
        public DateTime At { get; set; }
        public string Reason { get; set; } = "";
    }

    private sealed class AttemptRow
    {
        public int Attempt { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? FinishedAt { get; set; }
        public string? Outcome { get; set; }
        public string? Error { get; set; }
    }

    private sealed class SummaryRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public string State { get; set; } = "";
        public int Attempts { get; set; }
        public DateTime? RunAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }

    private sealed class StaleRow
    {
        public Guid Id { get; set; }
        public int Version { get; set; }
        public int Run { get; set; }
        public int Attempts { get; set; }
        public int MaxAttempts { get; set; }
        public bool CancelRequested { get; set; }
    }
}
