using BackgroundJobs.Processing;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace BackgroundJobs.Health;

/// Readiness: healthy only between "recovered and processing" and "stop requested".
/// Usable with ASP.NET health checks: services.AddHealthChecks().AddCheck&lt;JobServerHealthCheck&gt;("jobs").
public sealed class JobServerHealthCheck : IHealthCheck
{
    private readonly JobServer _server;

    internal JobServerHealthCheck(JobServer server) => _server = server;

    public bool IsReady => _server.IsReady;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(IsReady ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("job server is not processing"));
}
