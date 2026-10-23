using System.Text.Json;

namespace BackgroundJobs.Storage.InMemory;

/// IJobStore kept in this process, one lock instead of row locks. Rules come from JobTransition; this class only
/// implements the shared mechanics described on IJobStore.
internal sealed class InMemoryJobStore : IJobStore
{
    private readonly Lock _lock = new();
    private readonly Dictionary<Guid, Row> _jobs = [];
    private long _enqueueSeq;

    public Task MigrateAsync(CancellationToken ct) => Task.CompletedTask;

    public Task InsertAsync(NewJob job, CancellationToken ct)
    {
        var creation = job.Creation;
        lock (_lock)
        {
            var row = new Row
            {
                Id = job.Id, Name = job.Name, ArgsJson = job.ArgsJson, ArgsVersion = job.ArgsVersion,
                MaxAttempts = job.MaxAttempts, RetryDelayMs = job.RetryDelayMs, RetryJitterMs = job.RetryJitterMs,
                CreatedAt = job.Now,
            };
            Enter(row, creation, job.Now, creation.To == JobState.Scheduled ? job.RunAt : null);
            _jobs.Add(job.Id, row);
        }
        return Task.CompletedTask;
    }

    public Task<Job?> GetAsync(Guid id, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) return Task.FromResult<Job?>(null);
            return Task.FromResult<Job?>(new Job(
                r.Id, r.Name, JsonDocument.Parse(r.ArgsJson).RootElement.Clone(), r.ArgsVersion, r.State, r.Attempts, r.MaxAttempts,
                TimeSpan.FromMilliseconds(r.RetryDelayMs), r.State == JobState.Scheduled ? r.RunAt : null, r.CreatedAt, r.UpdatedAt,
                r.History.ToList(), r.AttemptLog.Select(a => a.ToPublic()).ToList(), r.RefundedAttempts));
        }
    }

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, CancellationToken ct)
    {
        lock (_lock)
        {
            var cursor = after?.ToString();
            IReadOnlyList<JobSummary> page = _jobs.Values
                .Where(r => cursor == null || string.CompareOrdinal(r.Key, cursor) > 0)
                .OrderBy(r => r.Key, StringComparer.Ordinal)
                .Take(limit)
                .Select(r => new JobSummary(r.Id, r.Name, r.State, r.Attempts, r.RunAt, r.CreatedAt, r.UpdatedAt))
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

    public Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        string nodeId, DateTime now, DateTime leaseUntil, int limit, IReadOnlyCollection<string> excludedNames, CancellationToken ct)
    {
        lock (_lock)
        {
            var ready = _jobs.Values
                .Where(r => JobTransition.Claim.Allows(r.State) && !excludedNames.Contains(r.Name))
                .OrderBy(r => r.EnqueuedAt).ThenBy(r => r.EnqueueSeq)
                .Take(limit)
                .ToList();
            IReadOnlyList<ClaimedJob> claimed = ready.Select(r =>
            {
                Apply(r, JobTransition.Claim, r.Version, null, now);
                r.Attempts++;
                r.LockedBy = nodeId;
                r.LeaseExpiresAt = leaseUntil;
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

    public Task<IReadOnlyCollection<Guid>> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct)
    {
        var cancelRequested = new List<Guid>();
        lock (_lock)
            foreach (var id in ids)
                if (_jobs.TryGetValue(id, out var r) && r.LockedBy == nodeId && r.State == JobState.Processing)
                {
                    r.LeaseExpiresAt = leaseUntil;
                    if (r.CancelRequestedAt != null) cancelRequested.Add(id);
                }
        return Task.FromResult<IReadOnlyCollection<Guid>>(cancelRequested);
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

    public Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct)
    {
        var recovered = 0;
        lock (_lock)
        {
            var stale = _jobs.Values
                .Where(r => r.State == JobState.Processing && (r.LeaseExpiresAt < now || (ownNodeId != null && r.LockedBy == ownNodeId)))
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

    public Task<QueueStats> QueueStatsAsync(CancellationToken ct)
    {
        lock (_lock)
        {
            var ready = _jobs.Values.Where(r => r.State == JobState.Enqueued).ToList();
            return Task.FromResult(new QueueStats(ready.Count, ready.Min(r => r.EnqueuedAt)));
        }
    }

    public Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct)
    {
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) throw new JobNotFoundException(id);
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
    }

    private sealed class Row
    {
        public Guid Id { get; init; }
        /// Text form: its ordinal order is the uuid order of the Postgres store.
        public string Key => Id.ToString();
        public required string Name { get; init; }
        public required string ArgsJson { get; init; }
        public int ArgsVersion { get; init; }
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
