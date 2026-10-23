namespace BackgroundJobs;

/// <summary>
/// A cron schedule: "minute hour day-of-month month day-of-week", or six fields with seconds first.
/// Fields take '*', '?', numbers, ranges (a-b), lists (a,b) and steps (*/n, a-b/n, a/n); months and days of the week
/// also take names (JAN, MON); Sunday is 0 or 7. When both day fields are restricted a day matching either one
/// matches (classic cron). Macros: @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly.
/// Occurrences are wall-clock times of a time zone: one that falls in a daylight-saving gap is skipped. In a repeated
/// hour a schedule that runs every hour (e.g. "*/5 * * * *") keeps running through both passes; any other schedule
/// (e.g. "30 1 * * *") runs only in the first.
/// </summary>
public sealed class CronExpression
{
    private static readonly string[] MonthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
    private static readonly string[] DayNames = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    /// <summary>
    /// A schedule that matches nothing within this many years matches nothing at all (e.g. "0 0 30 2 *").
    /// </summary>
    private const int SearchYears = 30;

    private readonly bool[] _seconds, _minutes, _hours, _daysOfMonth, _months, _daysOfWeek;
    private readonly bool _anyDayOfMonth, _anyDayOfWeek, _everyHour;

    /// <summary>
    /// The expression as given, trimmed (macros are not expanded).
    /// </summary>
    public string Expression { get; }

    /// <summary>
    /// Whether the expression has a seconds field.
    /// </summary>
    public bool HasSeconds { get; }

    private CronExpression(string expression, string[] fields)
    {
        Expression = expression;
        HasSeconds = fields.Length == 6;
        var i = HasSeconds ? 1 : 0;
        _seconds = HasSeconds ? ParseField(fields[0], 0, 59, null, "second") : Only(0, 59);
        _minutes = ParseField(fields[i], 0, 59, null, "minute");
        _hours = ParseField(fields[i + 1], 0, 23, null, "hour");
        _daysOfMonth = ParseField(fields[i + 2], 1, 31, null, "day of month");
        _months = ParseField(fields[i + 3], 1, 12, MonthNames, "month");
        _daysOfWeek = ParseField(fields[i + 4], 0, 7, DayNames, "day of week");
        if (_daysOfWeek[7]) _daysOfWeek[0] = true;
        _anyDayOfMonth = IsAny(fields[i + 2]);
        _anyDayOfWeek = IsAny(fields[i + 4]);
        _everyHour = _hours.All(h => h);
    }

    /// <exception cref="FormatException">The expression is not valid.</exception>
    public static CronExpression Parse(string expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var text = expression.Trim();
        var expanded = text.ToLowerInvariant() switch
        {
            "@yearly" or "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" or "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            _ => text,
        };
        var fields = expanded.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (fields.Length is not (5 or 6))
            throw new FormatException($"cron '{expression}' has {fields.Length} fields; use 5 (minute hour day-of-month month day-of-week, e.g. \"*/5 * * * *\"), 6 with seconds first, or a macro such as @daily");
        return new CronExpression(text, fields);
    }

    /// <summary>
    /// Like <see cref="Parse"/>, but returns false instead of throwing.
    /// </summary>
    public static bool TryParse(string expression, out CronExpression? cron)
    {
        try
        {
            cron = Parse(expression);
            return true;
        }
        catch (FormatException)
        {
            cron = null;
            return false;
        }
    }

    /// <summary>
    /// The first occurrence strictly after <paramref name="fromUtc"/>, in UTC; null when the schedule never matches.
    /// </summary>
    public DateTime? GetNextOccurrence(DateTime fromUtc, TimeZoneInfo timeZone)
    {
        ArgumentNullException.ThrowIfNull(timeZone);
        fromUtc = DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc);
        var local = TimeZoneInfo.ConvertTimeFromUtc(fromUtc, timeZone);
        // In a repeated hour the second pass of earlier wall-clock times still lies ahead of us.
        if (timeZone.IsAmbiguousTime(local))
        {
            var offsets = timeZone.GetAmbiguousTimeOffsets(local);
            local -= offsets.Max() - offsets.Min();
        }

        var step = HasSeconds ? TimeSpan.FromSeconds(1) : TimeSpan.FromMinutes(1);
        var t = Truncate(local);
        var limit = local.Year + SearchYears;
        DateTime? best = null;
        while (NextMatch(t, limit) is { } match)
        {
            var ambiguous = timeZone.IsAmbiguousTime(match);
            if (!timeZone.IsInvalidTime(match))
            {
                var offsets = !ambiguous ? [timeZone.GetUtcOffset(match)]
                    : _everyHour ? timeZone.GetAmbiguousTimeOffsets(match)
                    : [timeZone.GetAmbiguousTimeOffsets(match).Max()];
                foreach (var offset in offsets)
                {
                    var utc = DateTime.SpecifyKind(match - offset, DateTimeKind.Utc);
                    if (utc > fromUtc && (best == null || utc < best)) best = utc;
                }
            }
            // Past the repeated hour, later wall-clock times are later instants.
            if (best != null && !ambiguous) return best;
            t = match + step;
        }
        return best;
    }

    private DateTime Truncate(DateTime t) => HasSeconds
        ? new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second)
        : new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0);

    /// <summary>
    /// The first wall-clock time at or after <paramref name="t"/> the fields match.
    /// </summary>
    private DateTime? NextMatch(DateTime t, int limitYear)
    {
        while (t.Year <= limitYear)
        {
            if (!_months[t.Month]) t = new DateTime(t.Year, t.Month, 1).AddMonths(1);
            else if (!DayMatches(t)) t = t.Date.AddDays(1);
            else if (!_hours[t.Hour]) t = new DateTime(t.Year, t.Month, t.Day, t.Hour, 0, 0).AddHours(1);
            else if (!_minutes[t.Minute]) t = new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0).AddMinutes(1);
            else if (!_seconds[t.Second]) t = t.AddSeconds(1);
            else return t;
        }
        return null;
    }

    private bool DayMatches(DateTime t)
    {
        bool dom = _daysOfMonth[t.Day], dow = _daysOfWeek[(int)t.DayOfWeek];
        if (_anyDayOfMonth) return dow;
        if (_anyDayOfWeek) return dom;
        return dom || dow;
    }

    private static bool IsAny(string field) => field is "*" or "?";

    private static bool[] Only(int value, int max)
    {
        var set = new bool[max + 1];
        set[value] = true;
        return set;
    }

    private static bool[] ParseField(string field, int min, int max, string[]? names, string what)
    {
        var set = new bool[max + 1];
        foreach (var part in field.Split(','))
        {
            var (range, stepText) = part.Split('/') switch
            {
                [var r] => (r, null),
                [var r, var s] => (r, s),
                _ => throw Invalid(field, what, min, max),
            };
            int from, to;
            if (range is "*" or "?") (from, to) = (min, max);
            else if (range.Split('-') is [var a, var b]) (from, to) = (Value(a), Value(b));
            else
            {
                from = Value(range);
                // "a/n" runs from a to the end of the range.
                to = stepText == null ? from : max;
            }
            var step = 1;
            if (stepText != null && (!int.TryParse(stepText, out step) || step < 1)) throw Invalid(field, what, min, max);
            if (from > to) throw Invalid(field, what, min, max);
            for (var v = from; v <= to; v += step) set[v] = true;
        }
        return set;

        int Value(string text)
        {
            var index = names == null ? -1 : Array.IndexOf(names, text.ToUpperInvariant());
            var value = index >= 0 ? index + min : int.TryParse(text, out var n) ? n : -1;
            return value >= min && value <= max ? value : throw Invalid(field, what, min, max);
        }
    }

    private static FormatException Invalid(string field, string what, int min, int max) =>
        new($"cron {what} field '{field}' is invalid; use '*', a value from {min} to {max}, a range (a-b), a list (a,b) or a step (*/n)" +
            (what is "month" ? ", or names (JAN-DEC)" : what is "day of week" ? ", or names (SUN-SAT; Sunday is 0 or 7)" : ""));

    /// <summary>
    /// <see cref="Expression"/>.
    /// </summary>
    public override string ToString() => Expression;
}
