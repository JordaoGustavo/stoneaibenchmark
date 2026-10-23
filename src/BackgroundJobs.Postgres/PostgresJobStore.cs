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
        try { await ApplyMigrationsAsync(ct); }
        catch (NpgsqlException e) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"BackgroundJobs could not create or migrate its tables in PostgreSQL: {e.Message}. Check that the database in the connection " +
                "string given to UsePostgres(...) exists and is reachable, and that its user may CREATE TABLE in it; startup retries meanwhile", e);
        }
    }

    private async Task ApplyMigrationsAsync(CancellationToken ct)
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

    public async Task<Guid> InsertAsync(NewJob job, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        while (true)
        {
            await using var tx = await conn.BeginTransactionAsync(ct);
            var creation = job.Creation;
            if (job.ContinueAfter is { } parentId)
            {
                // FOR SHARE conflicts with the row lock every UPDATE of the parent takes: a parent finishing concurrently
                // either committed before (its finished state is read here) or waits for this insert to commit, and then
                // its release, a later statement with a fresh snapshot, sees the new awaiting job.
                var parentState = await conn.QuerySingleOrDefaultAsync<string?>(
                    "SELECT state FROM bj_jobs WHERE id = @parentId FOR SHARE", new { parentId }, tx);
                if (parentState == null) throw new JobNotFoundException(parentId);
                creation = JobTransition.CreateContinuation(JobStateWire.ParseState(parentState), job.Condition, job.RunAt > job.Now);
            }
            if (await InsertJobAsync(conn, tx, job, creation))
            {
                await tx.CommitAsync(ct);
                return job.Id;
            }
            // Read committed: this statement sees the holder the insert conflicted with, unless it finished meanwhile;
            // then the key is free and the insert is retried.
            if (await FindKeyHolderAsync(conn, job.IdempotencyKey!, tx) is { } holder) return holder;
        }
    }

    private static Task<Guid?> FindKeyHolderAsync(NpgsqlConnection conn, string key, NpgsqlTransaction? tx = null) =>
        conn.QuerySingleOrDefaultAsync<Guid?>($"SELECT id FROM bj_jobs WHERE idempotency_key = @key AND {KeyHeld}", new { key }, tx);

    /// <summary>
    /// The predicate of the bj_jobs_idempotency_key partial index: the job holds its key.
    /// </summary>
    private const string KeyHeld = "state IN ('awaiting', 'scheduled', 'enqueued', 'processing')";

    /// <summary>
    /// False, inserting nothing, when an unfinished job holds job.IdempotencyKey.
    /// </summary>
    private static async Task<bool> InsertJobAsync(NpgsqlConnection conn, NpgsqlTransaction tx, NewJob job, JobTransition? creation = null)
    {
        creation ??= job.Creation;
        var scheduled = creation.To == JobState.Scheduled;
        var enqueued = creation.To == JobState.Enqueued;
        // The conflict target names the partial unique index by its columns and predicate. A sequence value taken by a
        // conflicting insert is lost, which only leaves a gap in the FIFO tie-breaker.
        var inserted = await conn.ExecuteAsync($"""
            INSERT INTO bj_jobs (id, name, args, args_version, state, max_attempts, retry_delay_ms, retry_jitter_ms,
                                 run_at, enqueued_at, enqueue_seq, created_at, updated_at, queue, priority, idempotency_key,
                                 continue_after, continuation_condition, not_before, retry_backoff_multiplier, max_retry_delay_ms)
            VALUES (@Id, @Name, @ArgsJson::jsonb, @ArgsVersion, @State, @MaxAttempts, @RetryDelayMs, @RetryJitterMs,
                    @RunAt, @EnqueuedAt, CASE WHEN @Enqueued THEN nextval('bj_enqueue_seq') END, @Now, @Now, @Queue, @Priority,
                    @IdempotencyKey, @ContinueAfter, @Condition, @NotBefore, @RetryBackoffMultiplier, @MaxRetryDelayMs)
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND {KeyHeld} DO NOTHING
            """,
            new
            {
                job.Id, job.Name, job.ArgsJson, job.ArgsVersion, State = creation.To.ToWire(), job.MaxAttempts, job.RetryDelayMs, job.RetryJitterMs,
                RunAt = scheduled ? job.RunAt : null, EnqueuedAt = enqueued ? job.Now : (DateTime?)null, Enqueued = enqueued, job.Now,
                job.Queue, job.Priority, job.IdempotencyKey, job.ContinueAfter,
                Condition = job.ContinueAfter == null ? null : job.Condition.ToWire(), NotBefore = job.ContinueAfter == null ? null : job.RunAt,
                job.RetryBackoffMultiplier, job.MaxRetryDelayMs,
            }, tx);
        if (inserted == 0) return false;
        await InsertHistoryAsync(conn, tx, job.Id, creation.To, job.Now, creation.Reason);
        await NotifyAsync(conn, tx, creation.To);
        return true;
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
                a.Outcome == null ? null : JobStateWire.ParseOutcome(a.Outcome), a.Error)).ToList(),
            row.RefundedAttempts, row.Queue, row.Priority, row.IdempotencyKey)
        {
            ContinueAfter = row.ContinueAfter,
            Condition = row.ContinuationCondition == null ? null : JobStateWire.ParseCondition(row.ContinuationCondition),
        };
    }

    public async Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, bool descending, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var (op, dir) = descending ? ("<", "DESC") : (">", "ASC");
        var rows = await conn.QueryAsync<SummaryRow>($"""
            SELECT id, name, state, attempts, run_at, created_at, updated_at, queue, priority FROM bj_jobs
            WHERE @after::uuid IS NULL OR id {op} @after::uuid
            ORDER BY id {dir} LIMIT @limit
            """, new { after, limit });
        return rows.Select(r => new JobSummary(r.Id, r.Name, JobStateWire.ParseState(r.State), r.Attempts, r.RunAt, r.CreatedAt, r.UpdatedAt, r.Queue, r.Priority)).ToList();
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
        // State spelled out (= JobTransition.Promote.From) so the planner can use the bj_jobs_due partial index. Sequence
        // numbers are drawn in the select list over the sorted rows, so they follow run_at as in the in-memory store
        // (all promoted jobs share enqueued_at, so the sequence alone orders them).
        var promoted = await conn.ExecuteAsync("""
            WITH due AS (
                SELECT id, nextval('bj_enqueue_seq') AS seq FROM (
                    SELECT id FROM bj_jobs WHERE state = 'scheduled' AND run_at <= @now
                    ORDER BY run_at, id FOR UPDATE SKIP LOCKED
                ) sorted
            ), promoted AS (
                UPDATE bj_jobs j SET state = @to, run_at = NULL, enqueued_at = @now,
                       enqueue_seq = due.seq, version = j.version + 1, updated_at = @now
                FROM due WHERE j.id = due.id
                RETURNING j.id, due.seq
            )
            INSERT INTO bj_history (job_id, state, at, reason) SELECT id, @to, @now, @reason FROM promoted ORDER BY seq
            """, new { now, to = t.To.ToWire(), reason = t.Reason }, tx);
        if (promoted > 0) await NotifyAsync(conn, tx, t.To);
        await tx.CommitAsync(ct);
        return promoted;
    }

    public async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(ClaimRequest request, CancellationToken ct)
    {
        // A constant per order, never user input. State spelled out (= JobTransition.Claim.From) so the planner can prove
        // the partial indexes' predicate, prepared statements included: bj_jobs_ready or bj_jobs_ready_fifo.
        var order = request.Order == ClaimOrder.Priority ? "priority DESC, enqueued_at, enqueue_seq" : "enqueued_at, enqueue_seq";
        var limits = request.ConcurrencyLimits;
        // Only names this node can run (ClaimRequest.Names): a filter on the index scan, like @excluded; constant text.
        var onlyNames = request.Names == null ? "" : " AND name = ANY(@names)";
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Limited names: one claim at a time per name across all nodes (sorted, so two claims never wait on each other),
        // then their free slots, counted once the lock is held. Without limits the claim takes no lock.
        var freeSlots = new Dictionary<string, int>();
        if (limits.Count > 0)
        {
            var names = limits.Keys.Order(StringComparer.Ordinal).ToArray();
            foreach (var name in names)
                await conn.ExecuteAsync("SELECT pg_advisory_xact_lock(hashtextextended('bj_concurrency:' || @name, 0))", new { name }, tx);
            var running = (await conn.QueryAsync<(string Name, int Count)>(
                "SELECT name, count(*)::int FROM bj_jobs WHERE state = 'processing' AND name = ANY(@names) GROUP BY name", new { names }, tx))
                .ToDictionary(r => r.Name, r => r.Count);
            foreach (var (name, limit) in limits) freeSlots[name] = limit - running.GetValueOrDefault(name);
        }

        // Each pass claims in order, keeping per limited name only as many jobs as it has free slots. A job is dropped only
        // once its name used up its slots, so a short pass that filled no name found nothing more; one that did goes again
        // with that name excluded, so jobs over a limit never make a claim stop short. Every further pass fills a name.
        var claimed = new List<ClaimedJob>();
        while (true)
        {
            var atLimit = freeSlots.Where(f => f.Value <= 0).Select(f => f.Key).ToArray();
            var belowLimit = freeSlots.Where(f => f.Value > 0).ToArray();
            var wanted = request.Limit - claimed.Count;
            // One statement: claim, attempts++, open attempts, history. The lease is measured on the database's clock (see
            // IJobStore.RecoverAsync).
            var pass = (await conn.QueryAsync<ClaimRow>($"""
                WITH locked AS (
                    SELECT id, name, priority, enqueued_at, enqueue_seq FROM bj_jobs
                    WHERE queue = @queue AND state = 'enqueued' AND NOT (name = ANY(@excluded)){onlyNames}
                    ORDER BY {order}
                    LIMIT @limit FOR UPDATE SKIP LOCKED
                ), ranked AS (
                    SELECT locked.*, row_number() OVER (PARTITION BY name ORDER BY {order}) AS nth FROM locked
                ), kept AS (
                    SELECT ranked.* FROM ranked LEFT JOIN unnest(@limitNames::text[], @limitFree::int[]) AS f(name, free) USING (name)
                    WHERE f.free IS NULL OR nth <= f.free
                ), next AS (
                    SELECT id, (@attemptIds::uuid[])[row_number() OVER (ORDER BY {order})] AS attempt_id FROM kept
                ), claimed AS (
                    UPDATE bj_jobs j SET state = @to, attempts = j.attempts + 1, version = j.version + 1,
                           updated_at = @now, locked_by = @nodeId, lease_expires_at = now() + @lease
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
                       version, attempt_id, run, refunded_attempts, retry_backoff_multiplier, max_retry_delay_ms
                FROM claimed
                ORDER BY {order}
                """,
                new
                {
                    queue = request.Queue, nodeId = request.NodeId, now = request.Now, lease = request.LeaseDuration, limit = wanted,
                    attemptIds = Enumerable.Range(0, wanted).Select(_ => Guid.CreateVersion7()).ToArray(),
                    excluded = request.ExcludedNames.Concat(atLimit).ToArray(), names = request.Names?.ToArray() ?? [],
                    limitNames = belowLimit.Select(f => f.Key).ToArray(), limitFree = belowLimit.Select(f => f.Value).ToArray(),
                    to = JobTransition.Claim.To.ToWire(), reason = JobTransition.Claim.Reason,
                }, tx)).Select(r => r.ToClaimedJob()).ToList();

            claimed.AddRange(pass);
            foreach (var job in pass)
                if (freeSlots.TryGetValue(job.Name, out var slots)) freeSlots[job.Name] = slots - 1;
            if (claimed.Count == request.Limit || !belowLimit.Any(f => freeSlots[f.Key] <= 0)) break;
        }
        await tx.CommitAsync(ct);
        return claimed;
    }

    public async Task RegisterConsumerAsync(string nodeId, IReadOnlyCollection<string> queues, DateTime now, DateTime aliveUntil, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync("DELETE FROM bj_queue_consumers WHERE node_id = @nodeId OR alive_until < @now", new { nodeId, now }, tx);
        await conn.ExecuteAsync(
            "INSERT INTO bj_queue_consumers (queue, node_id, alive_until) SELECT unnest(@queues), @nodeId, @aliveUntil",
            new { queues = queues.ToArray(), nodeId, aliveUntil }, tx);
        await tx.CommitAsync(ct);
    }

    public async Task<IReadOnlyList<QueueStats>> QueueStatsAsync(DateTime now, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<(string Queue, long Ready, DateTime? OldestReadyAt, bool HasConsumer)>("""
            SELECT j.queue, count(*), min(j.enqueued_at),
                   EXISTS (SELECT 1 FROM bj_queue_consumers c WHERE c.queue = j.queue AND c.alive_until >= @now)
            FROM bj_jobs j WHERE j.state = 'enqueued' GROUP BY j.queue
            """, new { now });
        return rows.Select(r => new QueueStats(r.Queue, r.Ready, r.OldestReadyAt, r.HasConsumer)).ToList();
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

    /// <summary>
    /// Leases are measured on the database's clock; <paramref name="now"/> is not used (see IJobStore.RecoverAsync).
    /// </summary>
    public async Task<LeaseRenewal> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime now, TimeSpan leaseDuration, CancellationToken ct)
    {
        if (ids.Count == 0) return new LeaseRenewal([], []);
        await using var conn = await db.OpenConnectionAsync(ct);
        var renewed = (await conn.QueryAsync<(Guid Id, bool CancelRequested)>("""
            UPDATE bj_jobs SET lease_expires_at = now() + @lease WHERE id = ANY(@ids) AND locked_by = @nodeId AND state = 'processing'
            RETURNING id, cancel_requested_at IS NOT NULL
            """, new { lease = leaseDuration, ids = ids.ToArray(), nodeId })).AsList();
        return new LeaseRenewal(renewed.Select(r => r.Id).ToList(), renewed.Where(r => r.CancelRequested).Select(r => r.Id).ToList());
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

    /// <summary>
    /// Expiry is checked on the database's clock, the one leases are written with; <paramref name="now"/> only stamps
    /// the history and attempts.
    /// </summary>
    public async Task<int> RecoverAsync(DateTime now, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var stale = (await conn.QueryAsync<StaleRow>("""
            SELECT id, version, run, attempts - refunded_attempts AS attempts, max_attempts, cancel_requested_at IS NOT NULL AS cancel_requested FROM bj_jobs
            WHERE state = 'processing' AND lease_expires_at < now()
            FOR UPDATE SKIP LOCKED
            """, transaction: tx)).ToList();

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
        while (true)
        {
            await using var tx = await conn.BeginTransactionAsync(ct);
            // Locked, so a concurrent promotion, claim or purge either lands before (and is read here) or waits for this
            // transaction; unlocked, the version read could go stale and turn a valid transition into a conflict. A job
            // purged while this waited reads as missing (read committed re-checks the row once its lock is released).
            var current = await conn.QuerySingleOrDefaultAsync<(string State, int Version, string? IdempotencyKey)?>(
                "SELECT state, version, idempotency_key FROM bj_jobs WHERE id = @id FOR UPDATE", new { id }, tx);
            if (current is null) throw new JobNotFoundException(id);
            bool changed;
            try
            {
                changed = await TransitionAsync(conn, tx, id, current.Value.Version, transition, now);
            }
            catch (PostgresException e) when (e is { SqlState: PostgresErrorCodes.UniqueViolation, ConstraintName: "bj_jobs_idempotency_key" })
            {
                // A requeue whose key another unfinished job holds. Holder read after the rollback (the failed statement
                // aborted the transaction); when it finished meanwhile the key is free and the requeue is retried.
                await tx.RollbackAsync(ct);
                var key = current.Value.IdempotencyKey!;
                if (await FindKeyHolderAsync(conn, key) is { } holder) throw new IdempotencyKeyConflictException(id, key, holder);
                continue;
            }
            if (!changed) throw new InvalidJobTransitionException(id, JobStateWire.ParseState(current.Value.State), transition.Operation);
            await tx.CommitAsync(ct);
            return;
        }
    }

    /// <summary>
    /// The shared mechanics of IJobStore for one job: guarded by version, source state and owner; always writes history.
    /// </summary>
    private static async Task<bool> TransitionAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, int version, JobTransition t, DateTime now, DateTime? runAt = null, string? lockedBy = null)
    {
        var changed = await conn.ExecuteAsync("""
            UPDATE bj_jobs SET
                state = @to, version = version + 1, updated_at = @now,
                run_at = CASE WHEN @to = 'scheduled' THEN @runAt::timestamptz END,
                enqueued_at = CASE WHEN @to = 'enqueued' THEN @now ELSE enqueued_at END,
                enqueue_seq = CASE WHEN @to = 'enqueued' THEN nextval('bj_enqueue_seq') ELSE enqueue_seq END,
                attempts = CASE WHEN @resetAttempts THEN 0 ELSE attempts END,
                refunded_attempts = CASE WHEN @resetAttempts THEN 0 WHEN @refundAttempt THEN refunded_attempts + 1 ELSE refunded_attempts END,
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
        if (JobStateWire.Finished.Contains(t.To)) await ReleaseContinuationsAsync(conn, tx, id, t.To, now);
        return true;
    }

    /// <summary>
    /// JobTransition.ParentFinished for every awaiting continuation of the job that just finished, in the same transaction.
    /// Locks go parent first, then children (also down a chain), the order every path takes, so they cannot deadlock. A
    /// continuation inserted concurrently held the parent FOR SHARE, so this statement, run after the parent's UPDATE got
    /// its lock, sees it committed.
    /// </summary>
    private static async Task ReleaseContinuationsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid parentId, JobState parentState, DateTime now)
    {
        var children = (await conn.QueryAsync<AwaitingRow>(
            "SELECT id, version, continuation_condition, not_before FROM bj_jobs " +
            "WHERE continue_after = @parentId AND state = 'awaiting' ORDER BY id FOR UPDATE", new { parentId }, tx)).AsList();
        foreach (var c in children)
        {
            var transition = JobTransition.ParentFinished(parentState, JobStateWire.ParseCondition(c.ContinuationCondition), c.NotBefore > now);
            await TransitionAsync(conn, tx, c.Id, c.Version, transition, now, c.NotBefore);
        }
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

    private const string RecurringColumns =
        "id, job_name, cron, time_zone, args::text AS args_json, args_version, max_attempts, retry_delay_ms, retry_jitter_ms, " +
        "queue, priority, next_run_at, last_run_at, last_job_id, created_at, updated_at";

    public async Task<RecurringJob> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var row = await conn.QuerySingleAsync<RecurringRow>($"""
            INSERT INTO bj_recurring AS r (id, job_name, cron, time_zone, args, args_version, max_attempts, retry_delay_ms,
                                           retry_jitter_ms, queue, priority, next_run_at, created_at, updated_at)
            VALUES (@Id, @JobName, @Cron, @TimeZone, @ArgsJson::jsonb, @ArgsVersion, @MaxAttempts, @RetryDelayMs,
                    @RetryJitterMs, @Queue, @Priority, @NextRunAt, @Now, @Now)
            ON CONFLICT (id) DO UPDATE SET
                job_name = excluded.job_name, args = excluded.args, args_version = excluded.args_version,
                max_attempts = excluded.max_attempts, retry_delay_ms = excluded.retry_delay_ms,
                retry_jitter_ms = excluded.retry_jitter_ms, queue = excluded.queue, priority = excluded.priority,
                next_run_at = CASE WHEN r.cron = excluded.cron AND r.time_zone = excluded.time_zone THEN r.next_run_at
                                   ELSE excluded.next_run_at END,
                cron = excluded.cron, time_zone = excluded.time_zone, updated_at = excluded.updated_at
            RETURNING {RecurringColumns}
            """, d, tx);
        await NotifyAsync(conn, tx, JobState.Scheduled);
        await tx.CommitAsync(ct);
        return row.ToRecurringJob();
    }

    public async Task<RecurringJob?> GetRecurringAsync(string id, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<RecurringRow>($"SELECT {RecurringColumns} FROM bj_recurring WHERE id = @id", new { id });
        return row?.ToRecurringJob();
    }

    public async Task<IReadOnlyList<RecurringJob>> ListRecurringAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<RecurringRow>($"SELECT {RecurringColumns} FROM bj_recurring ORDER BY id COLLATE \"C\"");
        return rows.Select(r => r.ToRecurringJob()).ToList();
    }

    public async Task<bool> RemoveRecurringAsync(string id, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync("DELETE FROM bj_recurring WHERE id = @id", new { id }) > 0;
    }

    public async Task<IReadOnlyList<RecurringJob>> DueRecurringAsync(DateTime now, int limit, IReadOnlyCollection<string> excludedIds, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<RecurringRow>($"""
            SELECT {RecurringColumns} FROM bj_recurring WHERE next_run_at <= @now AND NOT (id = ANY(@excluded))
            ORDER BY next_run_at LIMIT @limit
            """, new { now, limit, excluded = excludedIds.ToArray() });
        return rows.Select(r => r.ToRecurringJob()).ToList();
    }

    public async Task<DateTime?> NextRecurringAtAsync(IReadOnlyCollection<string> excludedIds, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<DateTime?>(
            "SELECT min(next_run_at) FROM bj_recurring WHERE NOT (id = ANY(@excluded))", new { excluded = excludedIds.ToArray() });
    }

    public async Task<RecurringFireResult> FireRecurringAsync(RecurringFire fire, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        // The row lock serializes nodes firing the same occurrence; the loser sees next_run_at moved and writes nothing.
        var updated = await conn.ExecuteAsync("""
            UPDATE bj_recurring SET next_run_at = COALESCE(@NextRunAt, next_run_at), last_run_at = @now, last_job_id = @jobId
            WHERE id = @id AND updated_at = @updatedAt
              AND (@Occurrence::timestamptz IS NULL OR next_run_at = @Occurrence::timestamptz)
            """, new { id = fire.Definition.Id, updatedAt = fire.Definition.UpdatedAt, fire.Occurrence, fire.NextRunAt, now = fire.Job.Now, jobId = fire.Job.Id }, tx);
        if (updated == 0)
            return await conn.ExecuteScalarAsync<bool>("SELECT EXISTS (SELECT 1 FROM bj_recurring WHERE id = @id)", new { id = fire.Definition.Id }, tx)
                ? RecurringFireResult.Stale
                : RecurringFireResult.Gone;
        // Recurring jobs carry no idempotency key, so the insert never yields.
        await InsertJobAsync(conn, tx, fire.Job);
        await tx.CommitAsync(ct);
        return RecurringFireResult.Fired;
    }

    private static string[] Wire(IEnumerable<JobState> states) => states.Select(s => s.ToWire()).ToArray();

    /// <summary>
    /// Delivered on commit; Postgres folds identical notifications within a transaction.
    /// </summary>
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
        public string Queue { get; set; } = "";
        public int Priority { get; set; }
        public string? IdempotencyKey { get; set; }
        public Guid? ContinueAfter { get; set; }
        public string? ContinuationCondition { get; set; }
        public string State { get; set; } = "";
        public int Attempts { get; set; }
        public int RefundedAttempts { get; set; }
        public int MaxAttempts { get; set; }
        public int RetryDelayMs { get; set; }
        public DateTime? RunAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }

    private sealed class ClaimRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public string ArgsJson { get; set; } = "";
        public int ArgsVersion { get; set; }
        public int Attempt { get; set; }
        public int MaxAttempts { get; set; }
        public int RetryDelayMs { get; set; }
        public int RetryJitterMs { get; set; }
        public int Version { get; set; }
        public Guid AttemptId { get; set; }
        public int Run { get; set; }
        public int RefundedAttempts { get; set; }
        public double RetryBackoffMultiplier { get; set; } = 1;
        public int? MaxRetryDelayMs { get; set; }

        public ClaimedJob ToClaimedJob() => new(
            Id, Name, ArgsJson, ArgsVersion, Attempt, MaxAttempts, RetryDelayMs, RetryJitterMs, Version, AttemptId, Run, RefundedAttempts)
        {
            RetryBackoffMultiplier = RetryBackoffMultiplier, MaxRetryDelayMs = MaxRetryDelayMs,
        };
    }

    private sealed class RecurringRow
    {
        public string Id { get; set; } = "";
        public string JobName { get; set; } = "";
        public string Cron { get; set; } = "";
        public string TimeZone { get; set; } = "";
        public string ArgsJson { get; set; } = "";
        public int ArgsVersion { get; set; }
        public int MaxAttempts { get; set; }
        public int RetryDelayMs { get; set; }
        public int RetryJitterMs { get; set; }
        public string Queue { get; set; } = "";
        public int Priority { get; set; }
        public DateTime NextRunAt { get; set; }
        public DateTime? LastRunAt { get; set; }
        public Guid? LastJobId { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }

        public RecurringJob ToRecurringJob() => new(
            Id, JobName, Cron, TimeZone, JsonDocument.Parse(ArgsJson).RootElement.Clone(), ArgsVersion, MaxAttempts,
            TimeSpan.FromMilliseconds(RetryDelayMs), TimeSpan.FromMilliseconds(RetryJitterMs), Queue, Priority, NextRunAt, LastRunAt,
            LastJobId, CreatedAt, UpdatedAt);
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
        public string Queue { get; set; } = "";
        public int Priority { get; set; }
    }

    private sealed class AwaitingRow
    {
        public Guid Id { get; set; }
        public int Version { get; set; }
        public string ContinuationCondition { get; set; } = "";
        public DateTime? NotBefore { get; set; }
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
