using System.Text.Json;
using Dapper;
using Npgsql;

namespace BackgroundJobs.Storage.Postgres;

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
        var scheduled = job.RunAt > job.Now;
        var state = scheduled ? JobState.Scheduled : JobState.Enqueued;
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync("""
            INSERT INTO bj_jobs (id, name, args, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                                 run_at, enqueued_at, enqueue_seq, created_at, updated_at)
            VALUES (@Id, @Name, @ArgsJson::jsonb, @State, @MaxAttempts, @RetryDelayMs, @RetryJitterMs,
                    @RunAt, @EnqueuedAt, CASE WHEN @Scheduled THEN NULL ELSE nextval('bj_enqueue_seq') END, @Now, @Now)
            """,
            new
            {
                job.Id, job.Name, job.ArgsJson, State = state.ToWire(), job.MaxAttempts, job.RetryDelayMs, job.RetryJitterMs,
                RunAt = scheduled ? job.RunAt : null, EnqueuedAt = scheduled ? (DateTime?)null : job.Now, Scheduled = scheduled, job.Now,
            }, tx);
        await InsertHistoryAsync(conn, tx, job.Id, state, job.Now, scheduled ? Reasons.ScheduledAtCreation : Reasons.Created);
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
            row.Id, row.Name, JsonDocument.Parse(row.ArgsJson).RootElement.Clone(), state, row.Attempts, row.MaxAttempts,
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

    public async Task<int> PromoteDueAsync(DateTime now, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync("""
            WITH due AS (
                SELECT id FROM bj_jobs WHERE state = 'scheduled' AND run_at <= @now
                ORDER BY run_at, id FOR UPDATE SKIP LOCKED
            ), promoted AS (
                UPDATE bj_jobs j SET state = 'enqueued', run_at = NULL, enqueued_at = @now,
                       enqueue_seq = nextval('bj_enqueue_seq'), version = j.version + 1, updated_at = @now
                FROM due WHERE j.id = due.id
                RETURNING j.id
            )
            INSERT INTO bj_history (job_id, state, at, reason) SELECT id, 'enqueued', @now, @reason FROM promoted
            """, new { now, reason = Reasons.BecameDue });
    }

    public async Task<ClaimedJob?> ClaimAsync(string nodeId, DateTime now, DateTime leaseUntil, IReadOnlyCollection<string> excludedNames, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        // One statement = one transaction: claim, attempts++, open attempt, history.
        return await conn.QuerySingleOrDefaultAsync<ClaimedJob>("""
            WITH next AS (
                SELECT id FROM bj_jobs
                WHERE state = 'enqueued' AND NOT (name = ANY(@excluded))
                ORDER BY enqueued_at, enqueue_seq
                LIMIT 1 FOR UPDATE SKIP LOCKED
            ), claimed AS (
                UPDATE bj_jobs j SET state = 'processing', attempts = j.attempts + 1, version = j.version + 1,
                       updated_at = @now, locked_by = @nodeId, lease_expires_at = @leaseUntil
                FROM next WHERE j.id = next.id
                RETURNING j.*
            ), attempt AS (
                INSERT INTO bj_attempts (id, job_id, run, attempt, started_at)
                SELECT @attemptId, id, run, attempts, @now FROM claimed
            ), history AS (
                INSERT INTO bj_history (job_id, state, at, reason)
                SELECT id, 'processing', @now, @reason FROM claimed
            )
            SELECT id, name, args::text AS args_json, attempts AS attempt, max_attempts, retry_delay_ms, retry_jitter_ms,
                   version, @attemptId AS attempt_id
            FROM claimed
            """,
            new { nodeId, now, leaseUntil, excluded = excludedNames.ToArray(), attemptId = Guid.CreateVersion7(), reason = Reasons.Claimed });
    }

    public Task<bool> SucceedAsync(ClaimedJob job, string nodeId, DateTime finishedAt, CancellationToken ct) =>
        FinishAsync(job, AttemptOutcome.Succeeded, null,
            new Transition(job.Id, job.Version, Transitions.Finish, JobState.Succeeded, Reasons.Succeeded, finishedAt, LockedBy: nodeId), ct);

    public Task<bool> FailAsync(ClaimedJob job, string nodeId, string error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct) =>
        FinishAsync(job, AttemptOutcome.Failed, error,
            retryAt is null
                ? new Transition(job.Id, job.Version, Transitions.Finish, JobState.Failed, Reasons.MaxAttemptsExhausted, finishedAt, LockedBy: nodeId)
                : new Transition(job.Id, job.Version, Transitions.Finish, JobState.Scheduled, Reasons.RetryScheduled, finishedAt, RunAt: retryAt, LockedBy: nodeId),
            ct);

    private async Task<bool> FinishAsync(ClaimedJob job, AttemptOutcome outcome, string? error, Transition transition, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        if (!await TransitionAsync(conn, tx, transition)) return false;
        await conn.ExecuteAsync(
            "UPDATE bj_attempts SET finished_at = @at, outcome = @outcome, error = @error WHERE id = @id",
            new { at = transition.Now, outcome = outcome.ToWire(), error, id = job.AttemptId }, tx);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct)
    {
        if (ids.Count == 0) return;
        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(
            "UPDATE bj_jobs SET lease_expires_at = @leaseUntil WHERE id = ANY(@ids) AND locked_by = @nodeId AND state = 'processing'",
            new { leaseUntil, ids = ids.ToArray(), nodeId });
    }

    public async Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var stale = (await conn.QueryAsync<StaleRow>("""
            SELECT id, version, run, attempts, max_attempts FROM bj_jobs
            WHERE state = 'processing' AND (lease_expires_at < @now OR locked_by = @ownNodeId)
            FOR UPDATE SKIP LOCKED
            """, new { now, ownNodeId }, tx)).ToList();

        foreach (var job in stale)
        {
            await conn.ExecuteAsync(
                "UPDATE bj_attempts SET finished_at = @now, outcome = 'interrupted' WHERE job_id = @id AND run = @run AND finished_at IS NULL",
                new { now, job.Id, job.Run }, tx);
            var exhausted = job.Attempts >= job.MaxAttempts;
            await TransitionAsync(conn, tx, new Transition(job.Id, job.Version, Transitions.Finish,
                exhausted ? JobState.Failed : JobState.Enqueued,
                exhausted ? Reasons.MaxAttemptsExhausted : Reasons.RecoveredAfterCrash, now));
        }
        await tx.CommitAsync(ct);
        return stale.Count;
    }

    public Task DeleteAsync(Guid id, DateTime now, CancellationToken ct) =>
        UserTransitionAsync(id, "delete", v => new Transition(id, v, Transitions.Delete, JobState.Deleted, Reasons.DeletedByUser, now), ct);

    public Task RequeueAsync(Guid id, DateTime now, CancellationToken ct) =>
        UserTransitionAsync(id, "requeue", v => new Transition(id, v, Transitions.Requeue, JobState.Enqueued, Reasons.RequeuedByUser, now, ResetAttempts: true), ct);

    private async Task UserTransitionAsync(Guid id, string operation, Func<int, Transition> transition, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var current = await conn.QuerySingleOrDefaultAsync<(string State, int Version)?>(
            "SELECT state, version FROM bj_jobs WHERE id = @id", new { id }, tx);
        if (current is null) throw new JobNotFoundException(id);
        if (!await TransitionAsync(conn, tx, transition(current.Value.Version)))
            throw new InvalidJobTransitionException(id, JobStateWire.ParseState(current.Value.State), operation);
        await tx.CommitAsync(ct);
    }

    private sealed record Transition(
        Guid Id, int Version, JobState[] From, JobState To, string Reason, DateTime Now,
        DateTime? RunAt = null, bool ResetAttempts = false, string? LockedBy = null);

    /// The single place that changes a job's state: guarded by version and source state, always writes history.
    private static async Task<bool> TransitionAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Transition t)
    {
        var changed = await conn.ExecuteAsync("""
            UPDATE bj_jobs SET
                state = @to, version = version + 1, updated_at = @Now, run_at = @RunAt::timestamptz,
                enqueued_at = CASE WHEN @to = 'enqueued' THEN @Now ELSE enqueued_at END,
                enqueue_seq = CASE WHEN @to = 'enqueued' THEN nextval('bj_enqueue_seq') ELSE enqueue_seq END,
                attempts = CASE WHEN @ResetAttempts THEN 0 ELSE attempts END,
                run = CASE WHEN @ResetAttempts THEN run + 1 ELSE run END,
                locked_by = NULL, lease_expires_at = NULL
            WHERE id = @Id AND version = @Version AND state = ANY(@from)
              AND (@LockedBy::text IS NULL OR locked_by = @LockedBy::text)
            """,
            new { t.Id, t.Version, from = t.From.Select(s => s.ToWire()).ToArray(), to = t.To.ToWire(), t.Now, t.RunAt, t.ResetAttempts, t.LockedBy },
            tx);
        if (changed == 0) return false;
        await InsertHistoryAsync(conn, tx, t.Id, t.To, t.Now, t.Reason);
        return true;
    }

    private static Task InsertHistoryAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, JobState state, DateTime at, string reason) =>
        conn.ExecuteAsync("INSERT INTO bj_history (job_id, state, at, reason) VALUES (@id, @state, @at, @reason)",
            new { id, state = state.ToWire(), at, reason }, tx);

    private sealed class JobRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public string ArgsJson { get; set; } = "";
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
    }
}
