namespace BackgroundJobs;

/// The fixed vocabulary of history reasons.
public static class Reasons
{
    public const string Created = "created";
    public const string ScheduledAtCreation = "scheduled_at_creation";
    public const string BecameDue = "became_due";
    public const string Claimed = "claimed";
    public const string Succeeded = "succeeded";
    public const string RetryScheduled = "retry_scheduled";
    public const string MaxAttemptsExhausted = "max_attempts_exhausted";
    public const string RecoveredAfterCrash = "recovered_after_crash";
    public const string DeletedByUser = "deleted_by_user";
    public const string RequeuedByUser = "requeued_by_user";
}

/// One move of the job state machine: the states it may start from, where it goes and the history reason it records.
/// This is the only place the rules live; stores apply them (see IJobStore for the mechanics every store shares).
public sealed record JobTransition(string Operation, IReadOnlyList<JobState> From, JobState To, string Reason, bool ResetAttempts = false)
{
    public bool Allows(JobState state) => From.Contains(state);

    /// Not really a move (the job does not exist yet): the state and reason a new job starts with.
    public static JobTransition Create(bool scheduled) => scheduled
        ? new("create", [], JobState.Scheduled, Reasons.ScheduledAtCreation)
        : new("create", [], JobState.Enqueued, Reasons.Created);

    public static readonly JobTransition Promote = new("promote", [JobState.Scheduled], JobState.Enqueued, Reasons.BecameDue);
    public static readonly JobTransition Claim = new("claim", [JobState.Enqueued], JobState.Processing, Reasons.Claimed);
    public static readonly JobTransition Succeed = new("succeed", [JobState.Processing], JobState.Succeeded, Reasons.Succeeded);

    /// A failed attempt: back to scheduled for a retry, or failed for good.
    public static JobTransition Fail(bool retry) => retry
        ? new("fail", [JobState.Processing], JobState.Scheduled, Reasons.RetryScheduled)
        : new("fail", [JobState.Processing], JobState.Failed, Reasons.MaxAttemptsExhausted);

    /// An interrupted attempt counts against the budget: run again if any is left, otherwise fail.
    public static JobTransition Recover(int attempts, int maxAttempts) => attempts >= maxAttempts
        ? new("recover", [JobState.Processing], JobState.Failed, Reasons.MaxAttemptsExhausted)
        : new("recover", [JobState.Processing], JobState.Enqueued, Reasons.RecoveredAfterCrash);

    public static readonly JobTransition Delete =
        new("delete", [JobState.Scheduled, JobState.Enqueued], JobState.Deleted, Reasons.DeletedByUser);

    /// Runs a finished job again from attempt 1, keeping its history.
    public static readonly JobTransition Requeue =
        new("requeue", [JobState.Succeeded, JobState.Failed, JobState.Deleted], JobState.Enqueued, Reasons.RequeuedByUser, ResetAttempts: true);
}
