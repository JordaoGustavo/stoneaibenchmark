namespace BackgroundJobs.Storage;

// The storage SPI: what a store package (BackgroundJobs.Postgres, the in-memory store, ...) implements.
// Application code never uses these types; it talks to IJobClient.

public sealed record NewJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, DateTime? RunAt, DateTime Now,
    string Queue, int Priority)
{
    /// Whether the job starts scheduled (RunAt in the future) or enqueued.
    public JobTransition Creation => JobTransition.Create(RunAt > Now);
}

/// A job this node just moved to processing, with its open attempt.
public sealed record ClaimedJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int Attempt, int MaxAttempts, int RetryDelayMs, int RetryJitterMs,
    int Version, Guid AttemptId, int Run, int RefundedAttempts)
{
    /// Attempts that count against MaxAttempts, this one included.
    public int CountedAttempts => Attempt - RefundedAttempts;
}

/// Jobs of one queue ready to run right now, when the oldest of them became ready, and whether any live node consumes
/// the queue (see <see cref="IJobStore.RegisterConsumerAsync"/>).
public sealed record QueueStats(string Queue, long Ready, DateTime? OldestReadyAt, bool HasConsumer);

/// Priority: highest priority first, then FIFO. Fifo: FIFO alone, whatever the priority; the dispatcher uses it for a
/// share of the claims so low priorities are not starved (see BackgroundJobsOptions.FifoClaimEvery).
public enum ClaimOrder { Priority, Fifo }

/// One claim: up to <see cref="Limit"/> enqueued jobs of <see cref="Queue"/> in <see cref="Order"/>, skipping
/// <see cref="ExcludedNames"/>, locked to <see cref="NodeId"/> until <see cref="LeaseUntil"/>.
public sealed record ClaimRequest(
    string Queue, ClaimOrder Order, string NodeId, DateTime Now, DateTime LeaseUntil, int Limit, IReadOnlyCollection<string> ExcludedNames);

/// A recurring definition as written by <see cref="IJobStore.UpsertRecurringAsync"/>. NextRunAt is the occurrence computed
/// from <see cref="Now"/>; the store keeps the stored one instead when Cron and TimeZone did not change.
public sealed record RecurringDefinition(
    string Id, string JobName, string Cron, string TimeZone, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs,
    int RetryJitterMs, string Queue, int Priority, DateTime NextRunAt, DateTime Now);

/// Creates <see cref="Job"/> for recurring definition <see cref="RecurringId"/>. A scheduled fire (<see cref="Occurrence"/> set)
/// applies only while the definition's next_run_at still equals it, and moves next_run_at to <see cref="NextRunAt"/>; a
/// manual trigger (null) leaves next_run_at alone. Either way it sets last_run_at = Job.Now and last_job_id = Job.Id.
public sealed record RecurringFire(string RecurringId, DateTime? Occurrence, DateTime? NextRunAt, NewJob Job);

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
/// - entering enqueued sets enqueued_at and takes the next FIFO sequence number (queue and priority never change);
/// - <see cref="JobTransition.ResetAttempts"/> sets attempts and refunded attempts to 0 and starts a new run;
/// - <see cref="JobTransition.RefundAttempt"/> increments refunded attempts (attempt numbers keep growing);
/// - clears a pending cancel request;
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

    /// <see cref="JobTransition.Claim"/> for the jobs <paramref name="request"/> describes, in its order:
    /// <see cref="ClaimOrder.Priority"/> is priority descending, then FIFO (enqueued_at, sequence); <see cref="ClaimOrder.Fifo"/>
    /// is FIFO alone. Each claim increments attempts, locks the job until the lease ends and opens an attempt; all in one
    /// atomic step. Returned in claim order. Keep the ready jobs of each queue indexed in both orders, so a claim reads the
    /// head of one queue (docs/INTEGRACAO.md, "Escrevendo um novo store").
    Task<IReadOnlyList<ClaimedJob>> ClaimAsync(ClaimRequest request, CancellationToken ct);

    /// Records that <paramref name="nodeId"/> consumes <paramref name="queues"/> until <paramref name="aliveUntil"/>, replacing
    /// what it registered before. Called at start and on every heartbeat. A node that stops renewing counts as gone
    /// once <paramref name="aliveUntil"/> passes; registrations expired before <paramref name="now"/> may be dropped.
    Task RegisterConsumerAsync(string nodeId, IReadOnlyCollection<string> queues, DateTime now, DateTime aliveUntil, CancellationToken ct);

    /// Applies <paramref name="transition"/> (<see cref="JobTransition.Succeed"/>, <see cref="JobTransition.Fail"/> or <see cref="JobTransition.Interrupt"/>) if this
    /// node still owns the job, and closes its attempt with <paramref name="outcome"/>. False when the job was taken away.
    /// A pending cancel request wins over anything but success: the job is cancelled and its attempt closed as cancelled,
    /// so a request is not lost when the attempt ends before the owning node heard of it.
    Task<bool> FinishAsync(ClaimedJob job, string nodeId, JobTransition transition, AttemptOutcome outcome, string? error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct);

    /// Returns those of <paramref name="ids"/> a user asked to cancel (see <see cref="CancelAsync"/>).
    Task<IReadOnlyCollection<Guid>> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime leaseUntil, CancellationToken ct);

    /// Applies <see cref="JobTransition.Cancel"/> to a scheduled or enqueued job and returns false. For a processing job it
    /// only records the request, returned by <see cref="RenewLeasesAsync"/> to the owning node, and returns true. Recovery
    /// cancels instead of requeuing a job whose cancel was requested. Any transition out of processing clears the request.
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task<bool> CancelAsync(Guid id, DateTime now, CancellationToken ct);

    /// For every processing job whose lease expired (and, when given, every job held by <paramref name="ownNodeId"/>):
    /// closes its open attempts as interrupted and applies <see cref="JobTransition.Recover"/>; or, when a cancel was
    /// requested, as cancelled with <see cref="JobTransition.Cancel"/>.
    Task<int> RecoverAsync(DateTime now, string? ownNodeId, CancellationToken ct);

    /// One entry per queue that has ready jobs, whether or not this node consumes it. HasConsumer: a registration of
    /// <see cref="RegisterConsumerAsync"/> for the queue is still alive at <paramref name="now"/>.
    Task<IReadOnlyList<QueueStats>> QueueStatsAsync(DateTime now, CancellationToken ct);

    /// Applies a user transition (<see cref="JobTransition.Delete"/>, <see cref="JobTransition.Requeue"/>).
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct);

    /// Inserts or replaces a recurring definition, keeping created_at and last run, and next_run_at when cron and time
    /// zone are unchanged. Returns the stored definition. Wakes the schedulers of every node.
    Task<RecurringJob> UpsertRecurringAsync(RecurringDefinition definition, CancellationToken ct);

    Task<RecurringJob?> GetRecurringAsync(string id, CancellationToken ct);

    /// Ordered by id.
    Task<IReadOnlyList<RecurringJob>> ListRecurringAsync(CancellationToken ct);

    /// False when there was no such definition.
    Task<bool> RemoveRecurringAsync(string id, CancellationToken ct);

    /// Up to <paramref name="limit"/> definitions whose next_run_at is at or before <paramref name="now"/>, earliest first.
    Task<IReadOnlyList<RecurringJob>> DueRecurringAsync(DateTime now, int limit, CancellationToken ct);

    /// Earliest next_run_at among recurring definitions, or null when none.
    Task<DateTime?> NextRecurringAtAsync(CancellationToken ct);

    /// Applies <paramref name="fire"/> and inserts its job (as <see cref="InsertAsync"/>) in one atomic step, so that of
    /// several nodes firing the same occurrence exactly one creates a job. False when the definition is gone or, for a
    /// scheduled fire, another node fired that occurrence already; nothing is written then.
    Task<bool> FireRecurringAsync(RecurringFire fire, CancellationToken ct);

    /// Removes, with their history and attempts, up to <paramref name="limit"/> jobs in a
    /// <see cref="JobStateWire.Finished"/> state last updated before <paramref name="finishedBefore"/>. Returns how many.
    Task<int> PurgeFinishedAsync(DateTime finishedBefore, int limit, CancellationToken ct);
}
