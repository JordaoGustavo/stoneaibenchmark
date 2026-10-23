using System.Text.Json;
using BackgroundJobs.Processing;

namespace BackgroundJobs.Storage.InMemory;

/// IJobStore kept in this process: same transitions, history and FIFO order as the Postgres store, one lock instead
/// of row locks. Instead of LISTEN/NOTIFY it wakes the local loops directly.
internal sealed class InMemoryJobStore(Signals signals) : IJobStore
{
    private readonly Lock _lock = new();
    private readonly Dictionary<Guid, Row> _jobs = [];
    private long _enqueueSeq;

    public Task MigrateAsync(CancellationToken ct) => Task.CompletedTask;

    public Task InsertAsync(NewJob job, CancellationToken ct)
    {
        var scheduled = job.RunAt > job.Now;
        lock (_lock)
        {
            var row = new Row
            {
                Id = job.Id, Name = job.Name, ArgsJson = job.ArgsJson, ArgsVersion = job.ArgsVersion,
                MaxAttempts = job.MaxAttempts, RetryDelayMs = job.RetryDelayMs, RetryJitterMs = job.RetryJitterMs,
                CreatedAt = job.Now, UpdatedAt = job.Now,
            };
            if (scheduled) { row.State = JobState.Scheduled; row.RunAt = job.RunAt; }
            else Enqueue(row, job.Now);
            row.History.Add(new JobHistoryEntry(row.State, job.Now, scheduled ? Reasons.ScheduledAtCreation : Reasons.Created));
            _jobs.Add(job.Id, row);
        }
        Notify(scheduled ? JobState.Scheduled : JobState.Enqueued);
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
                r.History.ToList(), r.AttemptLog.Select(a => a.ToPublic()).ToList()));
        }
    }

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, CancellationToken ct)
    {
        lock (_lock)
        {
            // Ordinal order of the text form is Postgres' uuid order.
            var cursor = after?.ToString();
            IReadOnlyList<JobSummary> page = _jobs.Values
                .Select(r => (Key: r.Id.ToString(), Row: r))
                .Where(x => cursor == null || string.CompareOrdinal(x.Key, cursor) > 0)
                .OrderBy(x => x.Key, StringComparer.Ordinal)
                .Take(limit)
                .Select(x => new JobSummary(x.Row.Id, x.Row.Name, x.Row.State, x.Row.Attempts, x.Row.RunAt, x.Row.CreatedAt, x.Row.UpdatedAt))
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
            var due = _jobs.Values.Where(r => r.State == JobState.Scheduled && r.RunAt <= now)
                .OrderBy(r => r.RunAt).ThenBy(r => r.Id.ToString(), StringComparer.Ordinal).ToList();
            foreach (var r in due)
            {
                r.RunAt = null;
                Enqueue(r, now);
                r.Version++;
                r.UpdatedAt = now;
                r.History.Add(new JobHistoryEntry(JobState.Enqueued, now, Reasons.BecameDue));
            }
            promoted = due.Count;
        }
        if (promoted > 0) Notify(JobState.Enqueued);
        return Task.FromResult(promoted);
    }

    public Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        string nodeId, DateTime now, DateTime leaseUntil, int limit, IReadOnlyCollection<string> excludedNames, CancellationToken ct)
    {
        lock (_lock)
        {
            IReadOnlyList<ClaimedJob> claimed = _jobs.Values
                .Where(r => r.State == JobState.Enqueued && !excludedNames.Contains(r.Name))
                .OrderBy(r => r.EnqueuedAt).ThenBy(r => r.EnqueueSeq)
                .Take(limit)
                .ToList()
                .Select(r =>
                {
                    r.State = JobState.Processing;
                    r.Attempts++;
                    r.Version++;
                    r.UpdatedAt = now;
                    r.LockedBy = nodeId;
                    r.LeaseExpiresAt = leaseUntil;
                    var attempt = new AttemptRow(Guid.CreateVersion7(), r.Run, r.Attempts, now);
                    r.AttemptLog.Add(attempt);
                    r.History.Add(new JobHistoryEntry(JobState.Processing, now, Reasons.Claimed));
                    return new ClaimedJob(r.Id, r.Name, r.ArgsJson, r.ArgsVersion, r.Attempts, r.MaxAttempts, r.RetryDelayMs,
                        r.RetryJitterMs, r.Version, attempt.Id, r.Run);
                })
                .ToList();
            return Task.FromResult(claimed);
        }
    }

    public Task<bool> SucceedAsync(ClaimedJob job, string nodeId, DateTime finishedAt, CancellationToken ct) =>
        Task.FromResult(Finish(job, AttemptOutcome.Succeeded, null,
            new Transition(job.Id, job.Version, Transitions.Finish, JobState.Succeeded, Reasons.Succeeded, finishedAt, LockedBy: nodeId)));

    public Task<bool> FailAsync(ClaimedJob job, string nodeId, string error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct) =>
        Task.FromResult(Finish(job, AttemptOutcome.Failed, error,
            retryAt is null
                ? new Transition(job.Id, job.Version, Transitions.Finish, JobState.Failed, Reasons.MaxAttemptsExhausted, finishedAt, LockedBy: nodeId)
                : new Transition(job.Id, job.Version, Transitions.Finish, JobState.Scheduled, Reasons.RetryScheduled, finishedAt, RunAt: retryAt, LockedBy: nodeId)));

    private bool Finish(ClaimedJob job, AttemptOutcome outcome, string? error, Transition transition)
    {
        lock (_lock)
        {
            if (!TryTransition(transition)) return false;
            var attempt = _jobs[job.Id].AttemptLog.Single(a => a.Id == job.AttemptId);
            attempt.FinishedAt = transition.Now;
            attempt.Outcome = outcome;
            attempt.Error = error;
        }
        Notify(transition.To);
        return true;
    }

    public Task RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct)
    {
        lock (_lock)
            foreach (var id in ids)
                if (_jobs.TryGetValue(id, out var r) && r.LockedBy == nodeId && r.State == JobState.Processing)
                    r.LeaseExpiresAt = leaseUntil;
        return Task.CompletedTask;
    }

    public Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct)
    {
        List<Row> stale;
        lock (_lock)
        {
            stale = _jobs.Values
                .Where(r => r.State == JobState.Processing && (r.LeaseExpiresAt < now || (ownNodeId != null && r.LockedBy == ownNodeId)))
                .ToList();
            foreach (var r in stale)
            {
                foreach (var a in r.AttemptLog.Where(a => a.Run == r.Run && a.FinishedAt == null))
                {
                    a.FinishedAt = now;
                    a.Outcome = AttemptOutcome.Interrupted;
                }
                var exhausted = r.Attempts >= r.MaxAttempts;
                TryTransition(new Transition(r.Id, r.Version, Transitions.Finish,
                    exhausted ? JobState.Failed : JobState.Enqueued,
                    exhausted ? Reasons.MaxAttemptsExhausted : Reasons.RecoveredAfterCrash, now));
            }
        }
        if (stale.Count > 0) Notify(JobState.Enqueued);
        return Task.FromResult(stale.Count);
    }

    public Task<QueueStats> QueueStatsAsync(CancellationToken ct)
    {
        lock (_lock)
        {
            var ready = _jobs.Values.Where(r => r.State == JobState.Enqueued).ToList();
            return Task.FromResult(new QueueStats(ready.Count, ready.Min(r => r.EnqueuedAt)));
        }
    }

    public Task DeleteAsync(Guid id, DateTime now, CancellationToken ct) =>
        UserTransition(id, "delete", v => new Transition(id, v, Transitions.Delete, JobState.Deleted, Reasons.DeletedByUser, now));

    public Task RequeueAsync(Guid id, DateTime now, CancellationToken ct) =>
        UserTransition(id, "requeue", v => new Transition(id, v, Transitions.Requeue, JobState.Enqueued, Reasons.RequeuedByUser, now, ResetAttempts: true));

    private Task UserTransition(Guid id, string operation, Func<int, Transition> transition)
    {
        Transition t;
        lock (_lock)
        {
            if (!_jobs.TryGetValue(id, out var r)) throw new JobNotFoundException(id);
            t = transition(r.Version);
            if (!TryTransition(t)) throw new InvalidJobTransitionException(id, r.State, operation);
        }
        Notify(t.To);
        return Task.CompletedTask;
    }

    private sealed record Transition(
        Guid Id, int Version, JobState[] From, JobState To, string Reason, DateTime Now,
        DateTime? RunAt = null, bool ResetAttempts = false, string? LockedBy = null);

    /// Mirrors PostgresJobStore.TransitionAsync: guarded by version, source state and owner; always writes history.
    /// Callers hold the lock.
    private bool TryTransition(Transition t)
    {
        var r = _jobs[t.Id];
        if (r.Version != t.Version || !t.From.Contains(r.State) || (t.LockedBy != null && r.LockedBy != t.LockedBy)) return false;
        r.State = t.To;
        r.Version++;
        r.UpdatedAt = t.Now;
        r.RunAt = t.RunAt;
        if (t.To == JobState.Enqueued) Enqueue(r, t.Now);
        if (t.ResetAttempts) { r.Attempts = 0; r.Run++; }
        r.LockedBy = null;
        r.LeaseExpiresAt = null;
        r.History.Add(new JobHistoryEntry(t.To, t.Now, t.Reason));
        return true;
    }

    private void Enqueue(Row r, DateTime now)
    {
        r.State = JobState.Enqueued;
        r.EnqueuedAt = now;
        r.EnqueueSeq = ++_enqueueSeq;
    }

    private void Notify(JobState state)
    {
        if (state == JobState.Enqueued) signals.Dispatcher.Set();
        else if (state == JobState.Scheduled) signals.Scheduler.Set();
    }

    private sealed class Row
    {
        public Guid Id { get; init; }
        public required string Name { get; init; }
        public required string ArgsJson { get; init; }
        public int ArgsVersion { get; init; }
        public JobState State { get; set; }
        public int Attempts { get; set; }
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
        public DateTime CreatedAt { get; init; }
        public DateTime UpdatedAt { get; set; }
        public List<JobHistoryEntry> History { get; } = [];
        public List<AttemptRow> AttemptLog { get; } = [];
    }

    private sealed class AttemptRow(Guid id, int run, int attempt, DateTime startedAt)
    {
        public Guid Id => id;
        public int Run => run;
        public DateTime? FinishedAt { get; set; }
        public AttemptOutcome? Outcome { get; set; }
        public string? Error { get; set; }

        public JobAttempt ToPublic() => new(attempt, startedAt, FinishedAt, Outcome, Error);
    }
}
