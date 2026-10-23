namespace BackgroundJobs.Storage;

internal sealed record NewJob(
    Guid Id, string Name, string ArgsJson, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, DateTime? RunAt, DateTime Now);

/// A job this node just moved to processing, with its open attempt.
internal sealed record ClaimedJob(
    Guid Id, string Name, string ArgsJson, int Attempt, int MaxAttempts, int RetryDelayMs, int RetryJitterMs,
    int Version, Guid AttemptId);

internal interface IJobStore
{
    Task MigrateAsync(CancellationToken ct);

    Task InsertAsync(NewJob job, CancellationToken ct);
    Task<Job?> GetAsync(Guid id, CancellationToken ct);
    Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, CancellationToken ct);

    /// scheduled → enqueued for every job whose run_at has passed. Returns how many were promoted.
    Task<int> PromoteDueAsync(DateTime now, CancellationToken ct);

    /// enqueued → processing for up to <paramref name="limit"/> oldest ready jobs, opening attempt N+1 for each, in one transaction.
    /// Returned in FIFO order.
    Task<IReadOnlyList<ClaimedJob>> ClaimAsync(string nodeId, DateTime now, DateTime leaseUntil, int limit, IReadOnlyCollection<string> excludedNames, CancellationToken ct);

    /// processing → succeeded. False when this node no longer owns the job.
    Task<bool> SucceedAsync(ClaimedJob job, string nodeId, DateTime finishedAt, CancellationToken ct);

    /// processing → scheduled (retryAt set) or failed (retryAt null).
    Task<bool> FailAsync(ClaimedJob job, string nodeId, string error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct);

    Task RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct);

    /// Interrupts processing jobs whose lease expired (and, when given, every job held by <paramref name="ownNodeId"/>).
    Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct);

    Task DeleteAsync(Guid id, DateTime now, CancellationToken ct);
    Task RequeueAsync(Guid id, DateTime now, CancellationToken ct);
}
