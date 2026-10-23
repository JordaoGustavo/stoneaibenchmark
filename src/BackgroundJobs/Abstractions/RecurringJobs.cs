using System.Text.Json;
using System.Text.RegularExpressions;

namespace BackgroundJobs;

/// <summary>
/// Recurring jobs: a named definition that enqueues a job at every occurrence of a cron schedule.
/// Each occurrence is an ordinary job (retries, queues, history); occurrences never wait for the previous one.
/// Missed occurrences (no server running) fire once when a server comes back, then the schedule resumes from now.
/// </summary>
public interface IRecurringJobClient
{
    /// <summary>
    /// Creates or replaces the definition <paramref name="id"/>. Keeps the pending occurrence when the cron and time zone
    /// are unchanged, so re-declaring a definition at every start never skips a run.
    /// </summary>
    /// <exception cref="ArgumentException">Invalid id, cron, options, or a schedule that never fires.</exception>
    Task<RecurringJob> UpsertAsync<TArgs>(string id, string jobName, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default);

    /// <summary>
    /// The definition <paramref name="id"/>.
    /// </summary>
    /// <exception cref="RecurringJobNotFoundException"/>
    Task<RecurringJob> GetAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Ordered by id.
    /// </summary>
    Task<IReadOnlyList<RecurringJob>> ListAsync(CancellationToken ct = default);

    /// <summary>
    /// Stops future occurrences; jobs already created are left alone.
    /// </summary>
    /// <exception cref="RecurringJobNotFoundException"/>
    Task RemoveAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Enqueues one job now, outside the schedule (the next occurrence does not move). Returns its id.
    /// </summary>
    /// <exception cref="RecurringJobNotFoundException"/>
    Task<Guid> TriggerAsync(string id, CancellationToken ct = default);
}

/// <summary>
/// Options of the jobs a recurring definition creates. Unset fields fall back to <see cref="BackgroundJobsOptions"/>.
/// </summary>
public sealed record RecurringJobOptions
{
    /// <summary>
    /// The time zone the cron reads in. Defaults to UTC.
    /// </summary>
    public TimeZoneInfo? TimeZone { get; init; }

    /// <summary>See <see cref="JobOptions.MaxAttempts"/>.</summary>
    public int? MaxAttempts { get; init; }
    /// <summary>See <see cref="JobOptions.RetryDelay"/>.</summary>
    public TimeSpan? RetryDelay { get; init; }
    /// <summary>See <see cref="JobOptions.RetryJitter"/>.</summary>
    public TimeSpan? RetryJitter { get; init; }
    /// <summary>See <see cref="JobOptions.Queue"/>.</summary>
    public string? Queue { get; init; }
    /// <summary>See <see cref="JobOptions.Priority"/>.</summary>
    public int? Priority { get; init; }
}

/// <summary>
/// A stored recurring definition, with the options resolved against the server defaults.
/// </summary>
/// <param name="Id">See <see cref="RecurringJobIds.IsValid"/>.</param>
/// <param name="JobName">The registered job each occurrence enqueues.</param>
/// <param name="Cron">See <see cref="CronExpression.Parse"/>.</param>
/// <param name="TimeZone">Time zone id (IANA or Windows, as given).</param>
/// <param name="Args">Args of every job it creates.</param>
/// <param name="ArgsVersion">The args version <paramref name="Args"/> were stored with.</param>
/// <param name="MaxAttempts">Of every job it creates.</param>
/// <param name="RetryDelay">Of every job it creates.</param>
/// <param name="RetryJitter">Of every job it creates.</param>
/// <param name="Queue">Of every job it creates.</param>
/// <param name="Priority">Of every job it creates.</param>
/// <param name="NextRunAt">The pending occurrence, in UTC.</param>
/// <param name="LastRunAt">The last occurrence or trigger that created a job, in UTC.</param>
/// <param name="LastJobId">The job it created last.</param>
/// <param name="CreatedAt">UTC.</param>
/// <param name="UpdatedAt">UTC.</param>
public sealed record RecurringJob(
    string Id,
    string JobName,
    string Cron,
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

internal static class RecurringJobExtensions
{
    /// <summary>
    /// The job one occurrence of <paramref name="definition"/> enqueues.
    /// </summary>
    public static Storage.NewJob NewJob(this RecurringJob definition, DateTime now) => new(
        Processing.JobIds.Next(now), definition.JobName, definition.Args.GetRawText(), definition.ArgsVersion, definition.MaxAttempts,
        (int)definition.RetryDelay.TotalMilliseconds, (int)definition.RetryJitter.TotalMilliseconds, null, now, definition.Queue,
        definition.Priority);
}

/// <summary>
/// Validation of recurring definition ids.
/// </summary>
public static partial class RecurringJobIds
{
    /// <summary>
    /// 1 to 100 characters: letters, digits, '-', '_', '.' and ':', starting with a letter or digit.
    /// </summary>
    public static bool IsValid(string? id) => id != null && Pattern().IsMatch(id);

    internal static string Check(string id) => IsValid(id)
        ? id
        : throw new ArgumentException($"recurring job id '{id}' is invalid; use 1 to 100 characters of A-Z, a-z, 0-9, '-', '_', '.' and ':', starting with a letter or digit (e.g. \"nightly-report\")");

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$")]
    private static partial Regex Pattern();
}
