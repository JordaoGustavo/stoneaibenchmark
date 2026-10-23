using System.Text.Json;
using System.Text.RegularExpressions;

namespace BackgroundJobs;

/// Recurring jobs: a named definition that enqueues a job at every occurrence of a cron schedule.
/// Each occurrence is an ordinary job (retries, queues, history); occurrences never wait for the previous one.
/// Missed occurrences (no server running) fire once when a server comes back, then the schedule resumes from now.
public interface IRecurringJobClient
{
    /// Creates or replaces the definition <paramref name="id"/>. Keeps the pending occurrence when the cron and time zone
    /// are unchanged, so re-declaring a definition at every start never skips a run.
    /// <exception cref="ArgumentException">Invalid id, cron, options, or a schedule that never fires.</exception>
    Task<RecurringJob> UpsertAsync<TArgs>(string id, string jobName, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);

    /// <exception cref="RecurringJobNotFoundException"/>
    Task<RecurringJob> GetAsync(string id, CancellationToken ct = default);

    /// Ordered by id.
    Task<IReadOnlyList<RecurringJob>> ListAsync(CancellationToken ct = default);

    /// Stops future occurrences; jobs already created are left alone.
    /// <exception cref="RecurringJobNotFoundException"/>
    Task RemoveAsync(string id, CancellationToken ct = default);

    /// Enqueues one job now, outside the schedule (the next occurrence does not move). Returns its id.
    /// <exception cref="RecurringJobNotFoundException"/>
    Task<Guid> TriggerAsync(string id, CancellationToken ct = default);
}

/// Options of the jobs a recurring definition creates. Unset fields fall back to <see cref="BackgroundJobsOptions"/>.
public sealed record RecurringJobOptions
{
    /// The time zone the cron reads in. Defaults to UTC.
    public TimeZoneInfo? TimeZone { get; init; }

    public int? MaxAttempts { get; init; }
    public TimeSpan? RetryDelay { get; init; }
    public TimeSpan? RetryJitter { get; init; }
    public string? Queue { get; init; }
    public int? Priority { get; init; }
}

public sealed record RecurringJob(
    string Id,
    string JobName,
    string Cron,
    /// Time zone id (IANA or Windows, as given).
    string TimeZone,
    JsonElement Args,
    int ArgsVersion,
    int MaxAttempts,
    TimeSpan RetryDelay,
    TimeSpan RetryJitter,
    string Queue,
    int Priority,
    DateTime NextRunAt,
    DateTime? LastRunAt,
    Guid? LastJobId,
    DateTime CreatedAt,
    DateTime UpdatedAt);

public static partial class RecurringJobIds
{
    /// 1 to 100 characters: letters, digits, '-', '_', '.' and ':', starting with a letter or digit.
    public static bool IsValid(string? id) => id != null && Pattern().IsMatch(id);

    internal static string Check(string id) => IsValid(id)
        ? id
        : throw new ArgumentException($"recurring job id '{id}' is invalid: use 1 to 100 of A-Z, a-z, 0-9, '-', '_', '.', ':', starting with a letter or digit");

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$")]
    private static partial Regex Pattern();
}
