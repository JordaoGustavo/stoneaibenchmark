using System.Text.RegularExpressions;

namespace BackgroundJobs;

/// <summary>
/// Queue names and priorities. Both are kept small and plain so any store can index them: a queue name is safe as part
/// of a key (a Redis key, a Postgres value), and (priority, enqueue sequence) fits one sortable number (a Redis score).
/// </summary>
public static partial class JobQueues
{
    /// <summary>
    /// The queue of jobs that do not set <see cref="JobOptions.Queue"/>.
    /// </summary>
    public const string Default = "default";

    /// <summary>
    /// Jobs with a higher priority are claimed first; equal priorities run in enqueue order.
    /// </summary>
    public const int MinPriority = -100, MaxPriority = 100, DefaultPriority = 0;

    /// <summary>
    /// 1 to 64 characters: lowercase letters, digits, '-', '_' and '.', starting with a letter or digit.
    /// </summary>
    public static bool IsValidName(string? name) => name != null && NamePattern().IsMatch(name);

    internal const string NameRule = "use 1 to 64 characters of a-z, 0-9, '-', '_' and '.', starting with a letter or digit (e.g. \"emails\")";

    internal static string CheckName(string name) => IsValidName(name)
        ? name
        : throw new ArgumentException($"queue name '{name}' is invalid; {NameRule}. Fix the Queue option of the job or recurring definition");

    internal static int CheckPriority(int priority) => priority is >= MinPriority and <= MaxPriority
        ? priority
        : throw new ArgumentException(
            $"priority {priority} is out of range; set the Priority option to a value from {MinPriority} to {MaxPriority} (higher runs first)");

    [GeneratedRegex("^[a-z0-9][a-z0-9_.-]{0,63}$")]
    private static partial Regex NamePattern();
}
