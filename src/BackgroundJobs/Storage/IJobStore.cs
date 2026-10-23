namespace BackgroundJobs.Storage;

// The storage SPI: what a store package (BackgroundJobs.Postgres, the in-memory store, ...) implements.
// Application code never uses these types; it talks to IJobClient.

public sealed record NewJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, DateTime? RunAt, DateTime Now)
{
    /// Whether the job starts scheduled (RunAt in the future) or enqueued.
    public JobTransition Creation => JobTransition.Create(RunAt > Now);
}

/// A job this node just moved to processing, with its open attempt.
public sealed record ClaimedJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int Attempt, int MaxAttempts, int RetryDelayMs, int RetryJitterMs,
    int Version, Guid AttemptId, int Run);

/// Jobs ready to run right now and when the oldest of them became ready.
public sealed record QueueStats(long Ready, DateTime? OldestReadyAt);

/// Wakes this node's loops when another node changes the store (this node's own changes wake them already).
/// Stores that only live in this process need none.
public interface IJobStoreListener
{
    Task RunAsync(CancellationToken ct);
}

/// Lets a store wake this node's loops instead of waiting for the next poll.
public interface IJobSignals
{
    /// Something became enqueued: the dispatcher should claim.
    void JobsReady();

    /// Something became scheduled: the scheduler should recompute its next wake-up.
    void SchedulesChanged();
}

public static class JobSignalsExtensions
{
    /// The wake-up a transition into <paramref name="state"/> calls for, if any.
    public static void Entered(this IJobSignals signals, JobState state)
    {
        if (state == JobState.Enqueued) signals.JobsReady();
        else if (state == JobState.Scheduled) signals.SchedulesChanged();
    }
}

/// Persists jobs and applies <see cref="JobTransition"/>s atomically. Which transition happens, from which states,
/// with which history reason, is decided by <see cref="JobTransition"/>; a store only makes it durable.
/// Every transition, whatever the store:
/// - applies only if the job is in one of <see cref="JobTransition.From"/>, still has the version the caller read and,
///   when an owner is given, is still locked by that node; otherwise it changes nothing and reports false;
/// - bumps the version, sets updated_at, clears the lock and lease, sets run_at only when entering scheduled;
/// - entering enqueued sets enqueued_at and takes the next FIFO sequence number;
/// - <see cref="JobTransition.ResetAttempts"/> sets attempts to 0 and starts a new run;
/// - appends (To, now, Reason) to the history.
public interface IJobStore
{
    /// Creates or upgrades the schema. Called at every start, concurrently from several nodes.
    Task MigrateAsync(CancellationToken ct);

    /// Applies <see cref="NewJob.Creation"/>.
    Task InsertAsync(NewJob job, CancellationToken ct);
    Task<Job?> GetAsync(Guid id, CancellationToken ct);

    /// Ascending by id. For UUIDv7 ids that is creation order, and the order of their text form.
    Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, CancellationToken ct);

    /// Earliest run_at among scheduled jobs, or null when none.
    Task<DateTime?> NextRunAtAsync(CancellationToken ct);

    /// <see cref="JobTransition.Promote"/> for every scheduled job whose run_at has passed. Returns how many were promoted.
    Task<int> PromoteDueAsync(DateTime now, CancellationToken ct);

    /// <see cref="JobTransition.Claim"/> for up to <paramref name="limit"/> enqueued jobs in FIFO order (enqueued_at, sequence),
    /// skipping <paramref name="excludedNames"/>. Each claim increments attempts, locks the job to <paramref name="nodeId"/>
    /// until <paramref name="leaseUntil"/> and opens an attempt; all in one atomic step. Returned in FIFO order.
    Task<IReadOnlyList<ClaimedJob>> ClaimAsync(string nodeId, DateTime now, DateTime leaseUntil, int limit, IReadOnlyCollection<string> excludedNames, CancellationToken ct);

    /// Applies <paramref name="transition"/> (<see cref="JobTransition.Succeed"/>, <see cref="JobTransition.Fail"/> or <see cref="JobTransition.Interrupt"/>) if this
    /// node still owns the job, and closes its attempt with <paramref name="outcome"/>. False when the job was taken away.
    Task<bool> FinishAsync(ClaimedJob job, string nodeId, JobTransition transition, AttemptOutcome outcome, string? error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct);

    Task RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct);

    /// For every processing job whose lease expired (and, when given, every job held by <paramref name="ownNodeId"/>):
    /// closes its open attempts as interrupted and applies <see cref="JobTransition.Recover"/>.
    Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct);

    Task<QueueStats> QueueStatsAsync(CancellationToken ct);

    /// Applies a user transition (<see cref="JobTransition.Delete"/>, <see cref="JobTransition.Requeue"/>).
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct);

    /// Removes, with their history and attempts, up to <paramref name="limit"/> jobs in a
    /// <see cref="JobStateWire.Finished"/> state last updated before <paramref name="finishedBefore"/>. Returns how many.
    Task<int> PurgeFinishedAsync(DateTime finishedBefore, int limit, CancellationToken ct);
}
