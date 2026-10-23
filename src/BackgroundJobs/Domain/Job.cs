using System.Text.Json;

namespace BackgroundJobs;

/// <summary>
/// A job as listed by <see cref="IJobClient.ListAsync"/>: no args or history.
/// </summary>
/// <param name="Id">Version 7 GUID; ids sort in creation order.</param>
/// <param name="Name">The registered job name.</param>
/// <param name="State">Current state.</param>
/// <param name="Attempts">Executions so far in the current run.</param>
/// <param name="ScheduledAt">When a scheduled job becomes ready, in UTC; null otherwise.</param>
/// <param name="CreatedAt">UTC.</param>
/// <param name="UpdatedAt">Last state change, in UTC.</param>
/// <param name="Queue">The queue the job waits in.</param>
/// <param name="Priority">Higher runs first within its queue.</param>
public sealed record JobSummary(
    Guid Id, string Name, JobState State, int Attempts, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
    string Queue = JobQueues.Default, int Priority = JobQueues.DefaultPriority)
{
    /// <summary>The parent job of a continuation (<see cref="JobOptions.ContinueAfter"/>); null otherwise.</summary>
    public Guid? ContinueAfter { get; init; }
}

/// <summary>
/// A job with its args, state history and attempts, as returned by <see cref="IJobClient.GetAsync"/>.
/// </summary>
/// <param name="Id">Version 7 GUID; ids sort in creation order.</param>
/// <param name="Name">The registered job name.</param>
/// <param name="Args">The args as stored, in the shape of <paramref name="ArgsVersion"/>.</param>
/// <param name="ArgsVersion">The args version the job was enqueued with; see JobRegistrationOptions.ArgsVersion.</param>
/// <param name="State">Current state.</param>
/// <param name="Attempts">Executions so far in the current run; each attempt has its own number.</param>
/// <param name="MaxAttempts">Counted attempts allowed per run before the job fails for good.</param>
/// <param name="RetryDelay">Base delay before a retry.</param>
/// <param name="ScheduledAt">When a scheduled job becomes ready, in UTC; null otherwise.</param>
/// <param name="CreatedAt">UTC.</param>
/// <param name="UpdatedAt">Last state change, in UTC.</param>
/// <param name="History">State changes, oldest first.</param>
/// <param name="AttemptHistory">Attempts, oldest first; an unfinished one has no outcome yet.</param>
/// <param name="RefundedAttempts">Attempts of the current run given back because a server stopped; Attempts - RefundedAttempts counts against MaxAttempts.</param>
/// <param name="Queue">The queue the job waits in.</param>
/// <param name="Priority">Higher runs first within its queue.</param>
/// <param name="IdempotencyKey">See <see cref="JobOptions.IdempotencyKey"/>.</param>
public sealed record Job(
    Guid Id,
    string Name,
    JsonElement Args,
    int ArgsVersion,
    JobState State,
    int Attempts,
    int MaxAttempts,
    TimeSpan RetryDelay,
    DateTime? ScheduledAt,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<JobHistoryEntry> History,
    IReadOnlyList<JobAttempt> AttemptHistory,
    int RefundedAttempts = 0,
    string Queue = JobQueues.Default,
    int Priority = JobQueues.DefaultPriority,
    string? IdempotencyKey = null)
{
    /// <summary>The parent job of a continuation (<see cref="JobOptions.ContinueAfter"/>); null otherwise.</summary>
    public Guid? ContinueAfter { get; init; }

    /// <summary>The continuation condition when <see cref="ContinueAfter"/> is set; null otherwise.</summary>
    public ContinuationCondition? Condition { get; init; }

    /// <summary>Maximum random extra delay added to each retry (<see cref="JobOptions.RetryJitter"/>).</summary>
    public TimeSpan RetryJitter { get; init; }

    /// <summary>
    /// How the retry delay grows: <see cref="BackgroundJobs.RetryBackoff.Exponential"/> when
    /// <see cref="RetryBackoffMultiplier"/> is above 1, <see cref="BackgroundJobs.RetryBackoff.Fixed"/> otherwise.
    /// </summary>
    public RetryBackoff RetryBackoff => RetryBackoffMultiplier > 1 ? RetryBackoff.Exponential : RetryBackoff.Fixed;

    /// <summary>Each retry delay is the previous one times this; 1 is a fixed delay.</summary>
    public double RetryBackoffMultiplier { get; init; } = 1;

    /// <summary>Cap on the backed-off retry delay, before jitter; null for no cap (always null for a fixed delay).</summary>
    public TimeSpan? MaxRetryDelay { get; init; }
}

/// <summary>
/// One state change of a job.
/// </summary>
/// <param name="State">The state entered.</param>
/// <param name="At">UTC.</param>
/// <param name="Reason">Why, one of <see cref="Reasons"/>.</param>
public sealed record JobHistoryEntry(JobState State, DateTime At, string Reason);

/// <summary>
/// One execution of a job.
/// </summary>
/// <param name="Attempt">The attempt number; keeps growing across refunds.</param>
/// <param name="StartedAt">UTC.</param>
/// <param name="FinishedAt">UTC; null while running.</param>
/// <param name="Outcome">Null while running.</param>
/// <param name="Error">The exception text of a failed attempt; null otherwise.</param>
public sealed record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
