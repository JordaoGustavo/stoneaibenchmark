using System.Text.Json;

namespace BackgroundJobs.Storage.InMemory;

/// IJobStore kept in this process, one lock instead of row locks. Rules come from JobTransition; this class only
/// implements the shared mechanics described on IJobStore.
internal sealed class InMemoryJobStore : IJobStore
{
    private readonly Lock _lock = new();
    private readonly Dictionary<Guid, Row> _jobs = [];
    private long _enqueueSeq;
    /// (node, queue) → alive until.
    private readonly Dictionary<(string Node, string Queue), DateTime> _consumers = [];
    private readonly SortedDictionary<string, RecurringJob> _recurring = new(StringComparer.Ordinal);
    /// Idempotency key → the unfinished job holding it; kept by Enter.
    private readonly Dictionary<string, Guid> _keyHolders = new(StringComparer.Ordinal);

    public Task MigrateAsync(CancellationToken ct) => Task.CompletedTask;

    public Task<Guid> InsertAsync(NewJob job, CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(InsertLocked(job));
    }

    /// Callers hold the lock.
    private Guid InsertLocked(NewJob job)
    {
        if (job.IdempotencyKey != null && _keyHolders.TryGetValue(job.IdempotencyKey, out var holder)) return holder;
        var creation = job.Creation;
        var row = new Row
        {
            Id = job.Id, Name = job.Name, ArgsJson = job.ArgsJson, ArgsVersion = job.ArgsVersion,
            MaxAttempts = job.MaxAttempts, RetryDelayMs = job.RetryDelayMs, RetryJitterMs = job.RetryJitterMs,
            CreatedAt = job.Now, Queue = job.Queue, Priority = job.Priority, IdempotencyKey = job.IdempotencyKey,
        };
        Enter(row, creation, job.Now, creation.To == JobState.Scheduled ? job.RunAt : null);
        _jobs.Add(job.Id, row);
        return job.Id;
    }

    public Task<Job?> GetAsync(Guid id, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) return Task.FromResult<Job?>(null);
            return Task.FromResult<Job?>(new Job(
                r.Id, r.Name, JsonDocument.Parse(r.ArgsJson).RootElement.Clone(), r.ArgsVersion, r.State, r.Attempts, r.MaxAttempts,
                TimeSpan.FromMilliseconds(r.RetryDelayMs), r.State == JobState.Scheduled ? r.RunAt : null, r.CreatedAt, r.UpdatedAt,
                r.History.ToList(), r.AttemptLog.Select(a => a.ToPublic()).ToList(), r.RefundedAttempts, r.Queue, r.Priority, r.IdempotencyKey));
        }
    }

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, bool descending, CancellationToken ct)
    {
        lock (_lock)
        {
            var cursor = after?.ToString();
            var sign = descending ? -1 : 1;
            var matches = _jobs.Values.Where(r => cursor == null || sign * string.CompareOrdinal(r.Key, cursor) > 0);
            IReadOnlyList<JobSummary> page = (descending ? matches.OrderByDescending(r => r.Key, StringComparer.Ordinal) : matches.OrderBy(r => r.Key, StringComparer.Ordinal))
                .Take(limit)
                .Select(r => new JobSummary(r.Id, r.Name, r.State, r.Attempts, r.RunAt, r.CreatedAt, r.UpdatedAt, r.Queue, r.Priority))
                .ToList();
            return Task.FromResult(page);
        }
    }

    public Task<DateTime?> NextRunAtAsync(CancellationToken ct)
    {
        lock (_lock)
            return Task.FromResult(_jobs.Values.Where(r => r.State == JobState.Scheduled).Min(r => r.RunAt));
    }

    public Task<int> PromoteDueAsync(DateTime now, CancellationToken ct)
    {
        int promoted;
        lock (_lock)
        {
            var due = _jobs.Values.Where(r => JobTransition.Promote.Allows(r.State) && r.RunAt <= now)
                .OrderBy(r => r.RunAt).ThenBy(r => r.Key, StringComparer.Ordinal).ToList();
            foreach (var r in due) Apply(r, JobTransition.Promote, r.Version, null, now);
            promoted = due.Count;
        }
        return Task.FromResult(promoted);
    }

    public Task<IReadOnlyList<ClaimedJob>> ClaimAsync(ClaimRequest request, CancellationToken ct)
    {
        var (queue, order, nodeId, now, leaseDuration, limit, excludedNames, limits) = request;
        lock (_lock)
        {
            var candidates = _jobs.Values
                .Where(r => r.Queue == queue && JobTransition.Claim.Allows(r.State) && !excludedNames.Contains(r.Name));
            var ordered = (order == ClaimOrder.Priority ? candidates.OrderByDescending(r => r.Priority) : candidates.OrderBy(_ => 0))
                .ThenBy(r => r.EnqueuedAt).ThenBy(r => r.EnqueueSeq);
            // Free slots per limited name, spent as jobs are taken; a job whose name has none left is skipped.
            var freeSlots = limits.ToDictionary(
                l => l.Key, l => l.Value - _jobs.Values.Count(r => r.Name == l.Key && r.State == JobState.Processing));
            var ready = new List<Row>();
            foreach (var r in ordered)
            {
                if (ready.Count == limit) break;
                if (freeSlots.TryGetValue(r.Name, out var slots))
                {
                    if (slots <= 0) continue;
                    freeSlots[r.Name] = slots - 1;
                }
                ready.Add(r);
            }
            IReadOnlyList<ClaimedJob> claimed = ready.Select(r =>
            {
                Apply(r, JobTransition.Claim, r.Version, null, now);
                r.Attempts++;
                r.LockedBy = nodeId;
                r.LeaseExpiresAt = now + leaseDuration;
                var attempt = new AttemptRow(Guid.CreateVersion7(), r.Run, r.Attempts, now);
                r.AttemptLog.Add(attempt);
                return new ClaimedJob(r.Id, r.Name, r.ArgsJson, r.ArgsVersion, r.Attempts, r.MaxAttempts, r.RetryDelayMs,
                    r.RetryJitterMs, r.Version, attempt.Id, r.Run, r.RefundedAttempts);
            }).ToList();
            return Task.FromResult(claimed);
        }
    }

    public Task<bool> FinishAsync(
        ClaimedJob job, string nodeId, JobTransition transition, AttemptOutcome outcome, string? error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(job.Id, out var r) || r.LockedBy != nodeId) return Task.FromResult(false);
            if (r.CancelRequestedAt != null && transition != JobTransition.Succeed)
                (transition, outcome) = (JobTransition.Cancel, AttemptOutcome.Cancelled);
            if (!Apply(r, transition, job.Version, retryAt, finishedAt)) return Task.FromResult(false);
            r.AttemptLog.Single(a => a.Id == job.AttemptId).Close(finishedAt, outcome, error);
        }
        return Task.FromResult(true);
    }

    /// One process, one clock: the caller's now is the store's.
    public Task<LeaseRenewal> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime now, TimeSpan leaseDuration, CancellationToken ct)
    {
        List<Guid> renewed = [], cancelRequested = [];
        lock (_lock)
            foreach (var id in ids)
                if (_jobs.TryGetValue(id, out var r) && r.LockedBy == nodeId && r.State == JobState.Processing)
                {
                    r.LeaseExpiresAt = now + leaseDuration;
                    renewed.Add(id);
                    if (r.CancelRequestedAt != null) cancelRequested.Add(id);
                }
        return Task.FromResult(new LeaseRenewal(renewed, cancelRequested));
    }

    public Task<bool> CancelAsync(Guid id, DateTime now, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) throw new JobNotFoundException(id);
            if (r.State == JobState.Processing)
            {
                r.CancelRequestedAt ??= now;
                return Task.FromResult(true);
            }
            if (!Apply(r, JobTransition.Cancel, r.Version, null, now)) throw new InvalidJobTransitionException(id, r.State, JobTransition.Cancel.Operation);
        }
        return Task.FromResult(false);
    }

    public Task<int> RecoverAsync(DateTime now, CancellationToken ct)
    {
        var recovered = 0;
        lock (_lock)
        {
            var stale = _jobs.Values
                .Where(r => r.State == JobState.Processing && r.LeaseExpiresAt < now)
                .ToList();
            foreach (var r in stale)
            {
                var cancel = r.CancelRequestedAt != null;
                foreach (var a in r.AttemptLog.Where(a => a.Run == r.Run && a.FinishedAt == null))
                    a.Close(now, cancel ? AttemptOutcome.Cancelled : AttemptOutcome.Interrupted, null);
                var transition = cancel ? JobTransition.Cancel : JobTransition.Recover(r.Attempts - r.RefundedAttempts, r.MaxAttempts);
                if (Apply(r, transition, r.Version, null, now)) recovered++;
            }
        }
        return Task.FromResult(recovered);
    }

    public Task RegisterConsumerAsync(string nodeId, IReadOnlyCollection<string> queues, DateTime now, DateTime aliveUntil, CancellationToken ct)
    {
        lock (_lock)
        {
            foreach (var key in _consumers.Keys.Where(k => k.Node == nodeId || _consumers[k] < now).ToList()) _consumers.Remove(key);
            foreach (var queue in queues) _consumers[(nodeId, queue)] = aliveUntil;
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<QueueStats>> QueueStatsAsync(DateTime now, CancellationToken ct)
    {
        lock (_lock)
        {
            IReadOnlyList<QueueStats> stats = _jobs.Values.Where(r => r.State == JobState.Enqueued)
                .GroupBy(r => r.Queue)
                .Select(g => new QueueStats(g.Key, g.Count(), g.Min(r => r.EnqueuedAt),
                    _consumers.Any(c => c.Key.Queue == g.Key && c.Value >= now)))
                .ToList();
            return Task.FromResult(stats);
        }
    }

    public Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) throw new JobNotFoundException(id);
            if (transition.Allows(r.State) && !JobStateWire.Finished.Contains(transition.To) && r.IdempotencyKey != null
                && _keyHolders.TryGetValue(r.IdempotencyKey, out var holder) && holder != id)
                throw new IdempotencyKeyConflictException(id, r.IdempotencyKey, holder);
            if (!Apply(r, transition, r.Version, null, now)) throw new InvalidJobTransitionException(id, r.State, transition.Operation);
        }
        return Task.CompletedTask;
    }

    public Task<int> PurgeFinishedAsync(DateTime finishedBefore, int limit, CancellationToken ct)
    {
        lock (_lock)
        {
            var doomed = _jobs.Values.Where(r => JobStateWire.Finished.Contains(r.State) && r.UpdatedAt < finishedBefore)
                .OrderBy(r => r.UpdatedAt).Take(limit).Select(r => r.Id).ToList();
            foreach (var id in doomed) _jobs.Remove(id);
            return Task.FromResult(doomed.Count);
        }
    }

    public Task<RecurringJob> UpsertRecurringAsync(RecurringDefinition d, CancellationToken ct)
    {
        lock (_lock)
        {
            var existing = _recurring.GetValueOrDefault(d.Id);
            var keepNext = existing != null && existing.Cron == d.Cron && existing.TimeZone == d.TimeZone;
            var stored = new RecurringJob(
                d.Id, d.JobName, d.Cron, d.TimeZone, JsonDocument.Parse(d.ArgsJson).RootElement.Clone(), d.ArgsVersion, d.MaxAttempts,
                TimeSpan.FromMilliseconds(d.RetryDelayMs), TimeSpan.FromMilliseconds(d.RetryJitterMs), d.Queue, d.Priority,
                keepNext ? existing!.NextRunAt : d.NextRunAt, existing?.LastRunAt, existing?.LastJobId, existing?.CreatedAt ?? d.Now, d.Now);
            _recurring[d.Id] = stored;
            return Task.FromResult(stored);
        }
    }

    public Task<RecurringJob?> GetRecurringAsync(string id, CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(_recurring.GetValueOrDefault(id));
    }

    public Task<IReadOnlyList<RecurringJob>> ListRecurringAsync(CancellationToken ct)
    {
        lock (_lock) return Task.FromResult<IReadOnlyList<RecurringJob>>(_recurring.Values.ToList());
    }

    public Task<bool> RemoveRecurringAsync(string id, CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(_recurring.Remove(id));
    }

    public Task<IReadOnlyList<RecurringJob>> DueRecurringAsync(DateTime now, int limit, IReadOnlyCollection<string> excludedIds, CancellationToken ct)
    {
        lock (_lock)
            return Task.FromResult<IReadOnlyList<RecurringJob>>(_recurring.Values
                .Where(r => r.NextRunAt <= now && !excludedIds.Contains(r.Id)).OrderBy(r => r.NextRunAt).Take(limit).ToList());
    }

    public Task<DateTime?> NextRecurringAtAsync(IReadOnlyCollection<string> excludedIds, CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(_recurring.Values.Where(r => !excludedIds.Contains(r.Id)).Min(r => (DateTime?)r.NextRunAt));
    }

    public Task<RecurringFireResult> FireRecurringAsync(RecurringFire fire, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_recurring.TryGetValue(fire.Definition.Id, out var r)) return Task.FromResult(RecurringFireResult.Gone);
            if (r.UpdatedAt != fire.Definition.UpdatedAt || (fire.Occurrence is { } occurrence && r.NextRunAt != occurrence))
                return Task.FromResult(RecurringFireResult.Stale);
            _recurring[r.Id] = r with { NextRunAt = fire.NextRunAt ?? r.NextRunAt, LastRunAt = fire.Job.Now, LastJobId = fire.Job.Id };
            InsertLocked(fire.Job);
        }
        return Task.FromResult(RecurringFireResult.Fired);
    }

    /// The shared mechanics of IJobStore, guarded by source state and version. Callers hold the lock.
    private bool Apply(Row r, JobTransition t, int expectedVersion, DateTime? runAt, DateTime now)
    {
        if (r.Version != expectedVersion || !t.Allows(r.State)) return false;
        r.Version++;
        r.LockedBy = null;
        r.LeaseExpiresAt = null;
        r.CancelRequestedAt = null;
        if (t.ResetAttempts) { r.Attempts = 0; r.RefundedAttempts = 0; r.Run++; }
        else if (t.RefundAttempt) r.RefundedAttempts++;
        Enter(r, t, now, runAt);
        return true;
    }

    private void Enter(Row r, JobTransition t, DateTime now, DateTime? runAt)
    {
        r.State = t.To;
        r.UpdatedAt = now;
        r.RunAt = t.To == JobState.Scheduled ? runAt : null;
        if (t.To == JobState.Enqueued)
        {
            r.EnqueuedAt = now;
            r.EnqueueSeq = ++_enqueueSeq;
        }
        r.History.Add(new JobHistoryEntry(t.To, now, t.Reason));
        if (r.IdempotencyKey == null) return;
        if (!JobStateWire.Finished.Contains(t.To)) _keyHolders[r.IdempotencyKey] = r.Id;
        else if (_keyHolders.GetValueOrDefault(r.IdempotencyKey) == r.Id) _keyHolders.Remove(r.IdempotencyKey);
    }

    private sealed class Row
    {
        public Guid Id { get; init; }
        /// Text form: its ordinal order is the uuid order of the Postgres store.
        public string Key => Id.ToString();
        public required string Name { get; init; }
        public required string ArgsJson { get; init; }
        public int ArgsVersion { get; init; }
        public required string Queue { get; init; }
        public int Priority { get; init; }
        public string? IdempotencyKey { get; init; }
        public JobState State { get; set; }
        public int Attempts { get; set; }
        public int RefundedAttempts { get; set; }
        public int MaxAttempts { get; init; }
        public int RetryDelayMs { get; init; }
        public int RetryJitterMs { get; init; }
        public DateTime? RunAt { get; set; }
        public DateTime? EnqueuedAt { get; set; }
        public long EnqueueSeq { get; set; }
        public int Run { get; set; } = 1;
        public int Version { get; set; } = 1;
        public string? LockedBy { get; set; }
        public DateTime? LeaseExpiresAt { get; set; }
        public DateTime? CancelRequestedAt { get; set; }
        public DateTime CreatedAt { get; init; }
        public DateTime UpdatedAt { get; set; }
        public List<JobHistoryEntry> History { get; } = [];
        public List<AttemptRow> AttemptLog { get; } = [];
    }

    private sealed class AttemptRow(Guid id, int run, int attempt, DateTime startedAt)
    {
        public Guid Id => id;
        public int Run => run;
        public DateTime? FinishedAt { get; private set; }
        private AttemptOutcome? _outcome;
        private string? _error;

        public void Close(DateTime at, AttemptOutcome outcome, string? error)
        {
            FinishedAt = at;
            _outcome = outcome;
            _error = error;
        }

        public JobAttempt ToPublic() => new(attempt, startedAt, FinishedAt, _outcome, _error);
    }
}
