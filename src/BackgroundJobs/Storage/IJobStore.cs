namespace BackgroundJobs.Storage;

// The storage SPI: what a store package (BackgroundJobs.Postgres, the in-memory store, ...) implements.
// Application code never uses these types; it talks to IJobClient.

/// <summary>
/// A job to insert, with its options already resolved and validated.
/// </summary>
/// <param name="Id">Version 7 GUID generated from <paramref name="Now"/>.</param>
/// <param name="Name">Job name.</param>
/// <param name="ArgsJson">Args serialized with <see cref="JobJson.Options"/>.</param>
/// <param name="ArgsVersion">See <see cref="JobRegistrationOptions.ArgsVersion"/>.</param>
/// <param name="MaxAttempts">At least 1.</param>
/// <param name="RetryDelayMs">Delay before each retry.</param>
/// <param name="RetryJitterMs">Maximum random extra retry delay.</param>
/// <param name="RunAt">UTC; the job starts scheduled when it is after <paramref name="Now"/>.</param>
/// <param name="Now">The caller's clock, in UTC.</param>
/// <param name="Queue">A valid queue name.</param>
/// <param name="Priority">Within the queue's range.</param>
/// <param name="IdempotencyKey">See <see cref="JobOptions.IdempotencyKey"/>.</param>
public sealed record NewJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, DateTime? RunAt, DateTime Now,
    string Queue, int Priority, string? IdempotencyKey = null)
{
    /// <summary>
    /// Whether the job starts scheduled (RunAt in the future) or enqueued.
    /// </summary>
    public JobTransition Creation => JobTransition.Create(RunAt > Now);
}

/// <summary>
/// A job this node just moved to processing, with its open attempt.
/// </summary>
public sealed record ClaimedJob(
    Guid Id, string Name, string ArgsJson, int ArgsVersion, int Attempt, int MaxAttempts, int RetryDelayMs, int RetryJitterMs,
    int Version, Guid AttemptId, int Run, int RefundedAttempts)
{
    /// <summary>
    /// Attempts that count against MaxAttempts, this one included.
    /// </summary>
    public int CountedAttempts => Attempt - RefundedAttempts;
}

/// <summary>
/// Jobs of one queue ready to run right now, when the oldest of them became ready, and whether any live node consumes
/// the queue (see <see cref="IJobStore.RegisterConsumerAsync"/>).
/// </summary>
public sealed record QueueStats(string Queue, long Ready, DateTime? OldestReadyAt, bool HasConsumer);

/// <summary>
/// Priority: highest priority first, then FIFO. Fifo: FIFO alone, whatever the priority; the dispatcher uses it for a
/// share of the claims so low priorities are not starved (see BackgroundJobsOptions.FifoClaimEvery).
/// </summary>
public enum ClaimOrder
{
    /// <summary>Highest priority first, then FIFO.</summary>
    Priority,
    /// <summary>FIFO, whatever the priority.</summary>
    Fifo,
}

/// <summary>
/// One claim: up to <see cref="Limit"/> enqueued jobs of <see cref="Queue"/> in <see cref="Order"/>, skipping
/// <see cref="ExcludedNames"/>, locked to <see cref="NodeId"/> for <see cref="LeaseDuration"/> (see <see cref="IJobStore.RecoverAsync"/>
/// for which clock measures it). <see cref="ConcurrencyLimits"/>: job name → most jobs of that name processing at once, on
/// every node and in every queue; a name missing from it has no limit. <see cref="Names"/>, when set, restricts the claim
/// to those job names.
/// </summary>
public sealed record ClaimRequest(
    string Queue, ClaimOrder Order, string NodeId, DateTime Now, TimeSpan LeaseDuration, int Limit, IReadOnlyCollection<string> ExcludedNames,
    IReadOnlyDictionary<string, int> ConcurrencyLimits)
{
    /// <summary>
    /// Only jobs with one of these names are claimed (the names registered on the claiming node, so a node never claims a
    /// job it cannot run); jobs of other names are skipped like <see cref="ExcludedNames"/> and stay enqueued. Null claims
    /// any name; empty claims nothing.
    /// </summary>
    public IReadOnlyCollection<string>? Names { get; init; }
}

/// <summary>
/// What <see cref="IJobStore.RenewLeasesAsync"/> did: the ids whose lease it extended (the others are no longer this
/// node's), and those of them a user asked to cancel.
/// </summary>
public sealed record LeaseRenewal(IReadOnlyCollection<Guid> Renewed, IReadOnlyCollection<Guid> CancelRequested);

/// <summary>
/// A recurring definition as written by <see cref="IJobStore.UpsertRecurringAsync"/>. NextRunAt is the occurrence computed
/// from <see cref="Now"/>; the store keeps the stored one instead when Cron and TimeZone did not change.
/// </summary>
public sealed record RecurringDefinition(
    string Id, string JobName, string Cron, string TimeZone, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs,
    int RetryJitterMs, string Queue, int Priority, DateTime NextRunAt, DateTime Now);

/// <summary>
/// Creates <see cref="Job"/> for <see cref="Definition"/>, built from the definition as it was read. Applies only while
/// the stored definition was not upserted since (its updated_at still equals Definition.UpdatedAt), and, for a scheduled
/// fire (<see cref="Occurrence"/> set), while its next_run_at still equals Occurrence; it then moves next_run_at to
/// <see cref="NextRunAt"/>. A manual trigger (Occurrence null) leaves next_run_at alone. Either way it sets
/// last_run_at = Job.Now and last_job_id = Job.Id.
/// </summary>
public sealed record RecurringFire(RecurringJob Definition, DateTime? Occurrence, DateTime? NextRunAt, NewJob Job);

/// <summary>
/// Fired: the job was created. Stale: the definition was upserted, or the occurrence fired by another node, since it was
/// read; nothing was written. Gone: there is no such definition.
/// </summary>
public enum RecurringFireResult
{
    /// <summary>The job was created.</summary>
    Fired,
    /// <summary>The definition changed, or another node fired the occurrence; nothing was written.</summary>
    Stale,
    /// <summary>There is no such definition.</summary>
    Gone,
}

/// <summary>
/// Wakes this node's loops when another node changes the store (this node's own changes wake them already).
/// Stores that only live in this process need none.
/// </summary>
public interface IJobStoreListener
{
    /// <summary>
    /// Listens until <paramref name="ct"/> is cancelled, calling <see cref="IJobSignals"/> on changes. Should reconnect
    /// on its own; polling covers missed notifications.
    /// </summary>
    Task RunAsync(CancellationToken ct);
}

/// <summary>
/// Lets a store wake this node's loops instead of waiting for the next poll.
/// </summary>
public interface IJobSignals
{
    /// <summary>
    /// Something became enqueued: the dispatcher should claim.
    /// </summary>
    void JobsReady();

    /// <summary>
    /// Something became scheduled: the scheduler should recompute its next wake-up.
    /// </summary>
    void SchedulesChanged();
}

/// <summary>
/// Helpers for <see cref="IJobSignals"/>.
/// </summary>
public static class JobSignalsExtensions
{
    /// <summary>
    /// The wake-up a transition into <paramref name="state"/> calls for, if any.
    /// </summary>
    public static void Entered(this IJobSignals signals, JobState state)
    {
        if (state == JobState.Enqueued) signals.JobsReady();
        else if (state == JobState.Scheduled) signals.SchedulesChanged();
    }
}

/// <summary>
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
/// </summary>
public interface IJobStore
{
    /// <summary>
    /// Creates or upgrades the schema. Called at every start, concurrently from several nodes.
    /// </summary>
    Task MigrateAsync(CancellationToken ct);

    /// <summary>
    /// Applies <see cref="NewJob.Creation"/> and returns job.Id; or, when an unfinished job (not in
    /// <see cref="JobStateWire.Finished"/>) holds <see cref="NewJob.IdempotencyKey"/>, inserts nothing and returns that job's
    /// id. Atomic: of concurrent inserts with one key, exactly one creates a job.
    /// </summary>
    Task<Guid> InsertAsync(NewJob job, CancellationToken ct);

    /// <summary>
    /// Null when there is no such job.
    /// </summary>
    Task<Job?> GetAsync(Guid id, CancellationToken ct);

    /// <summary>
    /// By id, ascending unless <paramref name="descending"/>; <paramref name="after"/> is the last id seen in that direction.
    /// For UUIDv7 ids that is creation order, and the order of their text form.
    /// </summary>
    Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after, int limit, bool descending, CancellationToken ct);

    /// <summary>
    /// Earliest run_at among scheduled jobs, or null when none.
    /// </summary>
    Task<DateTime?> NextRunAtAsync(CancellationToken ct);

    /// <summary>
    /// <see cref="JobTransition.Promote"/> for every scheduled job whose run_at has passed. Returns how many were promoted.
    /// </summary>
    Task<int> PromoteDueAsync(DateTime now, CancellationToken ct);

    /// <summary>
    /// <see cref="JobTransition.Claim"/> for the jobs <paramref name="request"/> describes, in its order:
    /// <see cref="ClaimOrder.Priority"/> is priority descending, then FIFO (enqueued_at, sequence); <see cref="ClaimOrder.Fifo"/>
    /// is FIFO alone. A job whose name already has its <see cref="ClaimRequest.ConcurrencyLimits"/> jobs processing (counting
    /// those claimed by this call) is skipped and the claim keeps filling with the next ones; concurrent claims from any of
    /// the nodes together never exceed a limit. A job whose name is in ExcludedNames, or missing from
    /// <see cref="ClaimRequest.Names"/> when that is set, is skipped too.
    /// Each claim increments attempts, locks the job until the lease ends and opens an attempt; all in one
    /// atomic step. Returned in claim order. Keep the ready jobs of each queue indexed in both orders, so a claim reads the
    /// head of one queue (docs/INTEGRACAO.md, "Escrevendo um novo store").
    /// </summary>
    Task<IReadOnlyList<ClaimedJob>> ClaimAsync(ClaimRequest request, CancellationToken ct);

    /// <summary>
    /// Records that <paramref name="nodeId"/> consumes <paramref name="queues"/> until <paramref name="aliveUntil"/>, replacing
    /// what it registered before. Called at start and on every heartbeat. A node that stops renewing counts as gone
    /// once <paramref name="aliveUntil"/> passes; registrations expired before <paramref name="now"/> may be dropped.
    /// Empty <paramref name="queues"/> deregisters the node (on graceful stop).
    /// </summary>
    Task RegisterConsumerAsync(string nodeId, IReadOnlyCollection<string> queues, DateTime now, DateTime aliveUntil, CancellationToken ct);

    /// <summary>
    /// Applies <paramref name="transition"/> (<see cref="JobTransition.Succeed"/>, <see cref="JobTransition.Fail"/> or <see cref="JobTransition.Interrupt"/>) if this
    /// node still owns the job, and closes its attempt with <paramref name="outcome"/>. False when the job was taken away.
    /// A pending cancel request wins over anything but success: the job is cancelled and its attempt closed as cancelled,
    /// so a request is not lost when the attempt ends before the owning node heard of it.
    /// </summary>
    Task<bool> FinishAsync(ClaimedJob job, string nodeId, JobTransition transition, AttemptOutcome outcome, string? error, DateTime finishedAt, DateTime? retryAt, CancellationToken ct);

    /// <summary>
    /// Extends by <paramref name="leaseDuration"/> the lease of those of <paramref name="ids"/> still processing and locked by
    /// <paramref name="nodeId"/>, and reports which ones it extended and which a user asked to cancel (see <see cref="CancelAsync"/>).
    /// </summary>
    Task<LeaseRenewal> RenewLeasesAsync(string nodeId, IReadOnlyCollection<Guid> ids, DateTime now, TimeSpan leaseDuration, CancellationToken ct);

    /// <summary>
    /// Applies <see cref="JobTransition.Cancel"/> to a scheduled or enqueued job and returns false. For a processing job it
    /// only records the request, returned by <see cref="RenewLeasesAsync"/> to the owning node, and returns true. Recovery
    /// cancels instead of requeuing a job whose cancel was requested. Any transition out of processing clears the request.
    /// </summary>
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task<bool> CancelAsync(Guid id, DateTime now, CancellationToken ct);

    /// <summary>
    /// A store shared by several nodes measures leases on one clock of its own (both when writing them in
    /// <see cref="ClaimAsync"/> and <see cref="RenewLeasesAsync"/>, and when checking them here), never on the callers'
    /// <paramref name="now"/>: nodes' clocks drift apart, and a node behind would write leases another node finds expired.
    /// For every processing job whose lease expired:
    /// closes its open attempts as interrupted and applies <see cref="JobTransition.Recover"/>; or, when a cancel was
    /// requested, as cancelled with <see cref="JobTransition.Cancel"/>.
    /// </summary>
    Task<int> RecoverAsync(DateTime now, CancellationToken ct);

    /// <summary>
    /// One entry per queue that has ready jobs, whether or not this node consumes it. HasConsumer: a registration of
    /// <see cref="RegisterConsumerAsync"/> for the queue is still alive at <paramref name="now"/>.
    /// </summary>
    Task<IReadOnlyList<QueueStats>> QueueStatsAsync(DateTime now, CancellationToken ct);

    /// <summary>
    /// Applies a user transition (<see cref="JobTransition.Delete"/>, <see cref="JobTransition.Requeue"/>).
    /// </summary>
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    /// <exception cref="IdempotencyKeyConflictException">A requeue whose key another unfinished job holds; nothing changes.</exception>
    Task ApplyAsync(Guid id, JobTransition transition, DateTime now, CancellationToken ct);

    /// <summary>
    /// Inserts or replaces a recurring definition, keeping created_at and last run, and next_run_at when cron and time
    /// zone are unchanged. Returns the stored definition. Wakes the schedulers of every node.
    /// </summary>
    Task<RecurringJob> UpsertRecurringAsync(RecurringDefinition definition, CancellationToken ct);

    /// <summary>
    /// Null when there is no such definition.
    /// </summary>
    Task<RecurringJob?> GetRecurringAsync(string id, CancellationToken ct);

    /// <summary>
    /// Ordered by id.
    /// </summary>
    Task<IReadOnlyList<RecurringJob>> ListRecurringAsync(CancellationToken ct);

    /// <summary>
    /// False when there was no such definition.
    /// </summary>
    Task<bool> RemoveRecurringAsync(string id, CancellationToken ct);

    /// <summary>
    /// Up to <paramref name="limit"/> definitions whose next_run_at is at or before <paramref name="now"/>, earliest first,
    /// skipping <paramref name="excludedIds"/> (definitions this node cannot schedule).
    /// </summary>
    Task<IReadOnlyList<RecurringJob>> DueRecurringAsync(DateTime now, int limit, IReadOnlyCollection<string> excludedIds, CancellationToken ct);

    /// <summary>
    /// Earliest next_run_at among recurring definitions other than <paramref name="excludedIds"/>, or null when none.
    /// </summary>
    Task<DateTime?> NextRecurringAtAsync(IReadOnlyCollection<string> excludedIds, CancellationToken ct);

    /// <summary>
    /// Applies <paramref name="fire"/> and inserts its job (as <see cref="InsertAsync"/>) in one atomic step, so that of
    /// several nodes firing the same occurrence exactly one creates a job.
    /// </summary>
    Task<RecurringFireResult> FireRecurringAsync(RecurringFire fire, CancellationToken ct);

    /// <summary>
    /// Removes, with their history and attempts, up to <paramref name="limit"/> jobs in a
    /// <see cref="JobStateWire.Finished"/> state last updated before <paramref name="finishedBefore"/>. Returns how many.
    /// </summary>
    Task<int> PurgeFinishedAsync(DateTime finishedBefore, int limit, CancellationToken ct);
}
