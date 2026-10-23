namespace BackgroundJobs;

/// <summary>
/// Where a job is in its lifecycle. Transitions are defined by <see cref="JobTransition"/>.
/// </summary>
public enum JobState
{
    /// <summary>Waiting for its run time (a delayed job or a retry).</summary>
    Scheduled,
    /// <summary>Ready to be claimed by a worker consuming its queue.</summary>
    Enqueued,
    /// <summary>Claimed and leased by a node.</summary>
    Processing,
    /// <summary>Finished: an attempt returned without throwing.</summary>
    Succeeded,
    /// <summary>Finished: attempts exhausted.</summary>
    Failed,
    /// <summary>Finished: deleted by a user before it ran.</summary>
    Deleted,
    /// <summary>Finished: cancelled by a user.</summary>
    Cancelled,
}

/// <summary>
/// How one attempt ended.
/// </summary>
public enum AttemptOutcome
{
    /// <summary>The job returned.</summary>
    Succeeded,
    /// <summary>The job threw or timed out.</summary>
    Failed,
    /// <summary>Its node crashed or stopped while it ran.</summary>
    Interrupted,
    /// <summary>A user cancelled it while it ran.</summary>
    Cancelled,
}

/// <summary>
/// The lowercase names states and outcomes have on the wire (HTTP, store columns), and state predicates.
/// </summary>
public static class JobStateWire
{
    /// <summary>The lowercase wire name, e.g. "enqueued".</summary>
    public static string ToWire(this JobState s) => s.ToString().ToLowerInvariant();
    /// <summary>The lowercase wire name, e.g. "interrupted".</summary>
    public static string ToWire(this AttemptOutcome o) => o.ToString().ToLowerInvariant();
    /// <summary>Parses a wire name, ignoring case.</summary>
    /// <exception cref="ArgumentException">Not a state name.</exception>
    public static JobState ParseState(string s) => Enum.Parse<JobState>(s, ignoreCase: true);
    /// <summary>Parses a wire name, ignoring case.</summary>
    /// <exception cref="ArgumentException">Not an outcome name.</exception>
    public static AttemptOutcome ParseOutcome(string s) => Enum.Parse<AttemptOutcome>(s, ignoreCase: true);

    /// <summary>
    /// Succeeded, failed, deleted or cancelled: nothing will happen to the job unless a user requeues it. Subject to retention.
    /// </summary>
    public static readonly IReadOnlyList<JobState> Finished = [JobState.Succeeded, JobState.Failed, JobState.Deleted, JobState.Cancelled];

    /// <summary>
    /// Whether <see cref="IJobClient.DeleteAsync"/> accepts a job in this state.
    /// </summary>
    public static bool CanDelete(this JobState s) => JobTransition.Delete.Allows(s);

    /// <summary>
    /// Whether <see cref="IJobClient.CancelAsync"/> accepts a job in this state.
    /// </summary>
    public static bool CanCancel(this JobState s) => JobTransition.Cancel.Allows(s);

    /// <summary>
    /// Whether <see cref="IJobClient.RequeueAsync"/> accepts a job in this state.
    /// </summary>
    public static bool CanRequeue(this JobState s) => JobTransition.Requeue.Allows(s);
}
