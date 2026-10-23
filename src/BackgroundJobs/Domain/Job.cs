using System.Text.Json;

namespace BackgroundJobs;

public sealed record JobSummary(
    Guid Id, string Name, JobState State, int Attempts, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
    string Queue = JobQueues.Default, int Priority = JobQueues.DefaultPriority);

public sealed record Job(
    Guid Id,
    string Name,
    JsonElement Args,
    /// The args version the job was enqueued with; see JobRegistrationOptions.ArgsVersion.
    int ArgsVersion,
    JobState State,
    /// Executions so far in the current run; each attempt has its own number.
    int Attempts,
    int MaxAttempts,
    TimeSpan RetryDelay,
    DateTime? ScheduledAt,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<JobHistoryEntry> History,
    IReadOnlyList<JobAttempt> AttemptHistory,
    /// Attempts of the current run given back because a server stopped; Attempts - RefundedAttempts counts against MaxAttempts.
    int RefundedAttempts = 0,
    string Queue = JobQueues.Default,
    int Priority = JobQueues.DefaultPriority);

public sealed record JobHistoryEntry(JobState State, DateTime At, string Reason);

public sealed record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
