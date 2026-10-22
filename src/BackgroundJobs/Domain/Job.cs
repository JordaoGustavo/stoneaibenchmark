using System.Text.Json;

namespace BackgroundJobs;

public sealed record JobSummary(Guid Id, string Name, JobState State, int Attempts, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt);

public sealed record Job(
    Guid Id,
    string Name,
    JsonElement Args,
    JobState State,
    int Attempts,
    int MaxAttempts,
    TimeSpan RetryDelay,
    DateTime? ScheduledAt,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<JobHistoryEntry> History,
    IReadOnlyList<JobAttempt> AttemptHistory);

public sealed record JobHistoryEntry(JobState State, DateTime At, string Reason);

public sealed record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
