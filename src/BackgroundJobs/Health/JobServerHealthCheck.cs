using BackgroundJobs.Processing;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Health;

/// Unhealthy outside "recovered and processing" → "stop requested". Degraded when the ready queue is stalled
/// (a job ready for longer than StalledQueueThreshold) or a job circuit breaker is open. Half-open lets a trial through,
/// so it does not degrade: otherwise a type that is never enqueued again would stay degraded forever.
/// Usable with ASP.NET health checks: services.AddHealthChecks().AddCheck&lt;JobServerHealthCheck&gt;("jobs").
public sealed class JobServerHealthCheck : IHealthCheck
{
    private readonly JobServer _server;
    private readonly JobCircuitBreaker _breakers;
    private readonly QueueMonitor _queue;
    private readonly IOptions<BackgroundJobsOptions> _options;
    private readonly TimeProvider _time;

    internal JobServerHealthCheck(
        JobServer server, JobCircuitBreaker breakers, QueueMonitor queue, IOptions<BackgroundJobsOptions> options, TimeProvider time)
    {
        _server = server;
        _breakers = breakers;
        _queue = queue;
        _options = options;
        _time = time;
    }

    public bool IsReady => _server.IsReady;

    public JobServerHealthReport Report()
    {
        var threshold = _options.Value.StalledQueueThreshold;
        var sample = _queue.Latest;
        var queue = new QueueHealth(
            sample?.Ready, sample?.OldestReadyAge, sample?.At, sample != null && sample.OldestReadyAge > threshold, threshold);
        var breakers = _breakers.Snapshot()
            .Select(b => new CircuitBreakerHealth(b.Job, b.State, b.ConsecutiveFailures, b.OpenUntil)).ToList();
        var status = !IsReady ? HealthStatus.Unhealthy
            : queue.Stalled || breakers.Any(b => b.State == "open") ? HealthStatus.Degraded
            : HealthStatus.Healthy;
        return new JobServerHealthReport(status, IsReady, queue, breakers, _time.GetUtcNow());
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var report = Report();
        var data = new Dictionary<string, object>
        {
            ["ready"] = report.Ready,
            ["queue.ready"] = report.Queue.Ready ?? -1,
            ["queue.oldestReadyAgeSeconds"] = report.Queue.OldestReadyAge?.TotalSeconds ?? -1,
            ["queue.stalled"] = report.Queue.Stalled,
            ["circuitBreakers"] = report.CircuitBreakers.ToDictionary(b => b.Job, b => (object)b.State),
        };
        return Task.FromResult(report.Status switch
        {
            HealthStatus.Unhealthy => HealthCheckResult.Unhealthy("job server is not processing", data: data),
            HealthStatus.Degraded => HealthCheckResult.Degraded(Describe(report), data: data),
            _ => HealthCheckResult.Healthy(data: data),
        });
    }

    private static string Describe(JobServerHealthReport report) => string.Join("; ", new[]
    {
        report.Queue.Stalled ? $"queue stalled: oldest ready job waiting {report.Queue.OldestReadyAge!.Value.TotalSeconds:0}s" : null,
        report.CircuitBreakers.Any(b => b.State == "open")
            ? "circuit breakers open: " + string.Join(", ", report.CircuitBreakers.Where(b => b.State == "open").Select(b => b.Job))
            : null,
    }.Where(s => s != null));
}

public sealed record JobServerHealthReport(
    HealthStatus Status, bool Ready, QueueHealth Queue, IReadOnlyList<CircuitBreakerHealth> CircuitBreakers, DateTimeOffset At);

/// Ready/OldestReadyAge/SampledAt are null until the queue was sampled once.
public sealed record QueueHealth(long? Ready, TimeSpan? OldestReadyAge, DateTimeOffset? SampledAt, bool Stalled, TimeSpan StalledThreshold);

/// State: closed, open or half-open.
public sealed record CircuitBreakerHealth(string Job, string State, int ConsecutiveFailures, DateTimeOffset? OpenUntil);
