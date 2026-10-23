namespace BackgroundJobs;

/// <summary>
/// Settings of <see cref="DashboardEndpoints.MapBackgroundJobsDashboard"/>.
/// </summary>
public sealed class DashboardOptions
{
    /// <summary>
    /// Zone used to display times until a viewer picks their own (kept in a cookie). Defaults to UTC.
    /// </summary>
    public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Utc;
}
