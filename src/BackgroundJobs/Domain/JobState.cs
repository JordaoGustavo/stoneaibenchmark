namespace BackgroundJobs;

public enum JobState { Scheduled, Enqueued, Processing, Succeeded, Failed, Deleted }

public enum AttemptOutcome { Succeeded, Failed, Interrupted }

public static class JobStateWire
{
    public static string ToWire(this JobState s) => s.ToString().ToLowerInvariant();
    public static string ToWire(this AttemptOutcome o) => o.ToString().ToLowerInvariant();
    public static JobState ParseState(string s) => Enum.Parse<JobState>(s, ignoreCase: true);
    public static AttemptOutcome ParseOutcome(string s) => Enum.Parse<AttemptOutcome>(s, ignoreCase: true);

    /// Succeeded, failed or deleted: nothing will happen to the job unless a user requeues it. Subject to retention.
    public static readonly IReadOnlyList<JobState> Finished = [JobState.Succeeded, JobState.Failed, JobState.Deleted];

    /// Whether <see cref="IJobClient.DeleteAsync"/> accepts a job in this state.
    public static bool CanDelete(this JobState s) => JobTransition.Delete.Allows(s);

    /// Whether <see cref="IJobClient.RequeueAsync"/> accepts a job in this state.
    public static bool CanRequeue(this JobState s) => JobTransition.Requeue.Allows(s);
}
