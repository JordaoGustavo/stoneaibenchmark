using System.Text.RegularExpressions;

namespace BackgroundJobs;

/// Queue names and priorities. Both are kept small and plain so any store can index them: a queue name is safe as part
/// of a key (a Redis key, a Postgres value), and (priority, enqueue sequence) fits one sortable number (a Redis score).
public static partial class JobQueues
{
    public const string Default = "default";

    /// Jobs with a higher priority are claimed first; equal priorities run in enqueue order.
    public const int MinPriority = -100, MaxPriority = 100, DefaultPriority = 0;

    /// 1 to 64 characters: lowercase letters, digits, '-', '_' and '.', starting with a letter or digit.
    public static bool IsValidName(string? name) => name != null && NamePattern().IsMatch(name);

    internal static string CheckName(string name) => IsValidName(name)
        ? name
        : throw new ArgumentException($"queue '{name}' is invalid: use 1 to 64 of a-z, 0-9, '-', '_', '.', starting with a letter or digit");

    internal static int CheckPriority(int priority) => priority is >= MinPriority and <= MaxPriority
        ? priority
        : throw new ArgumentException($"priority must be between {MinPriority} and {MaxPriority}");

    [GeneratedRegex("^[a-z0-9][a-z0-9_.-]{0,63}$")]
    private static partial Regex NamePattern();
}
