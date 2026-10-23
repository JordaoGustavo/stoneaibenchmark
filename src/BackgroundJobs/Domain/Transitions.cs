namespace BackgroundJobs;

/// <summary>
/// The fixed vocabulary of history reasons.
/// </summary>
public static class Reasons
{
    /// <summary>Enqueued at creation.</summary>
    public const string Created = "created";
    /// <summary>Scheduled at creation (RunAt in the future).</summary>
    public const string ScheduledAtCreation = "scheduled_at_creation";
    /// <summary>A scheduled job's run time passed; it was enqueued.</summary>
    public const string BecameDue = "became_due";
    /// <summary>A worker claimed it.</summary>
    public const string Claimed = "claimed";
    /// <summary>An attempt returned.</summary>
    public const string Succeeded = "succeeded";
    /// <summary>An attempt failed; a retry is scheduled.</summary>
    public const string RetryScheduled = "retry_scheduled";
    /// <summary>An attempt failed or was interrupted with no attempts left.</summary>
    public const string MaxAttemptsExhausted = "max_attempts_exhausted";
    /// <summary>Its node's lease expired (crash); it was enqueued again.</summary>
    public const string RecoveredAfterCrash = "recovered_after_crash";
    /// <summary>Its server stopped; it was enqueued again with the attempt given back.</summary>
    public const string InterruptedByShutdown = "interrupted_by_shutdown";
    /// <summary>A user deleted it.</summary>
    public const string DeletedByUser = "deleted_by_user";
    /// <summary>A user requeued it.</summary>
    public const string RequeuedByUser = "requeued_by_user";
    /// <summary>A user cancelled it.</summary>
    public const string CancelledByUser = "cancelled_by_user";
}

/// <summary>
/// One move of the job state machine: the states it may start from, where it goes and the history reason it records.
/// This is the only place the rules live; stores apply them (see IJobStore for the mechanics every store shares).
/// </summary>
/// <param name="Operation">Verb used in errors, e.g. "requeue".</param>
/// <param name="From">The states it applies from.</param>
/// <param name="To">The state it moves to.</param>
/// <param name="Reason">The history reason it records, one of <see cref="Reasons"/>.</param>
/// <param name="ResetAttempts">Starts a new run: attempts and refunded attempts go back to 0.</param>
/// <param name="RefundAttempt">The attempt just made does not count against MaxAttempts (it keeps its number).</param>
public sealed record JobTransition(
    string Operation, IReadOnlyList<JobState> From, JobState To, string Reason, bool ResetAttempts = false, bool RefundAttempt = false)
{
    /// <summary>Whether a job in <paramref name="state"/> can make this move.</summary>
    public bool Allows(JobState state) => From.Contains(state);

    /// <summary>
    /// Not really a move (the job does not exist yet): the state and reason a new job starts with.
    /// </summary>
    public static JobTransition Create(bool scheduled) => scheduled
        ? new("create", [], JobState.Scheduled, Reasons.ScheduledAtCreation)
        : new("create", [], JobState.Enqueued, Reasons.Created);

    /// <summary>A scheduled job whose run time passed becomes enqueued.</summary>
    public static readonly JobTransition Promote = new("promote", [JobState.Scheduled], JobState.Enqueued, Reasons.BecameDue);
    /// <summary>A worker takes an enqueued job.</summary>
    public static readonly JobTransition Claim = new("claim", [JobState.Enqueued], JobState.Processing, Reasons.Claimed);
    /// <summary>An attempt returned.</summary>
    public static readonly JobTransition Succeed = new("succeed", [JobState.Processing], JobState.Succeeded, Reasons.Succeeded);

    /// <summary>
    /// A failed attempt: back to scheduled for a retry, or failed for good.
    /// </summary>
    public static JobTransition Fail(bool retry) => retry
        ? new("fail", [JobState.Processing], JobState.Scheduled, Reasons.RetryScheduled)
        : new("fail", [JobState.Processing], JobState.Failed, Reasons.MaxAttemptsExhausted);

    /// <summary>
    /// An interrupted attempt counts against the budget: run again if any is left, otherwise fail.
    /// <paramref name="attempts"/> are the counted ones: attempts minus refunded attempts.
    /// </summary>
    public static JobTransition Recover(int attempts, int maxAttempts) => attempts >= maxAttempts
        ? new("recover", [JobState.Processing], JobState.Failed, Reasons.MaxAttemptsExhausted)
        : new("recover", [JobState.Processing], JobState.Enqueued, Reasons.RecoveredAfterCrash);

    /// <summary>
    /// The job stopped because its server is stopping. Unlike a crash it is never the job's fault, so it is queued again
    /// at once and the attempt, still recorded, is given back: a deploy must not use up a job's budget.
    /// </summary>
    public static readonly JobTransition Interrupt =
        new("interrupt", [JobState.Processing], JobState.Enqueued, Reasons.InterruptedByShutdown, RefundAttempt: true);

    /// <summary>
    /// Stops a job for good. A processing job is not moved directly: its node is asked to cancel it, then finishes it
    /// with this transition (see IJobStore.CancelAsync).
    /// </summary>
    public static readonly JobTransition Cancel =
        new("cancel", [JobState.Scheduled, JobState.Enqueued, JobState.Processing], JobState.Cancelled, Reasons.CancelledByUser);

    /// <summary>
    /// Removes a job that has not started; it stays visible as deleted until retention purges it.
    /// </summary>
    public static readonly JobTransition Delete =
        new("delete", [JobState.Scheduled, JobState.Enqueued], JobState.Deleted, Reasons.DeletedByUser);

    /// <summary>
    /// Runs a finished job again from attempt 1, keeping its history.
    /// </summary>
    public static readonly JobTransition Requeue =
        new("requeue", [JobState.Succeeded, JobState.Failed, JobState.Deleted, JobState.Cancelled], JobState.Enqueued, Reasons.RequeuedByUser, ResetAttempts: true);

    /// <summary>
    /// The states the operation named <paramref name="operation"/> applies from; null for an unknown name.
    /// </summary>
    internal static IReadOnlyList<JobState>? FromOf(string operation) => operation switch
    {
        "promote" => Promote.From,
        "claim" => Claim.From,
        "succeed" or "fail" or "recover" or "interrupt" => Succeed.From,
        "cancel" => Cancel.From,
        "delete" => Delete.From,
        "requeue" => Requeue.From,
        _ => null,
    };
}
