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

/// Which states each transition may start from. Storage enforces these in the UPDATE's WHERE clause.
internal static class Transitions
{
    public static readonly JobState[] Delete = [JobState.Scheduled, JobState.Enqueued];
    public static readonly JobState[] Requeue = [JobState.Succeeded, JobState.Failed, JobState.Deleted];
    public static readonly JobState[] Promote = [JobState.Scheduled];
    public static readonly JobState[] Claim = [JobState.Enqueued];
    public static readonly JobState[] Finish = [JobState.Processing];
}
