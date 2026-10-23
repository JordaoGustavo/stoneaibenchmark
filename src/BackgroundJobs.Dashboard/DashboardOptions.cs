namespace BackgroundJobs;

public sealed class DashboardOptions
{
    /// Zone used to display times until a viewer picks their own (kept in a cookie). Defaults to UTC.
    public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Utc;
}
