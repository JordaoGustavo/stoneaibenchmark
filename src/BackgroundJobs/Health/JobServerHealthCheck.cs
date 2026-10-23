using BackgroundJobs.Processing;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Health;

/// <summary>
/// Unhealthy outside "recovered and processing" → "stop requested", and when MaxAbandonedJobs jobs run without a worker
/// (the next stuck job will keep a worker for good). Degraded when the ready queue is stalled (a job ready for longer
/// than StalledQueueThreshold), a job circuit breaker is open, or a job ran without a worker for longer than
/// AbandonedJobAlertAfter. Half-open lets a trial through,
/// so it does not degrade: otherwise a type that is never enqueued again would stay degraded forever.
/// Usable with ASP.NET health checks: services.AddHealthChecks().AddCheck&lt;JobServerHealthCheck&gt;("jobs").
/// </summary>
public sealed class JobServerHealthCheck : IHealthCheck
{
    private readonly JobServer _server;
    private readonly JobCircuitBreaker _breakers;
    private readonly QueueMonitor _queue;
    private readonly AbandonedJobs _abandoned;
    private readonly IOptions<BackgroundJobsOptions> _options;
    private readonly TimeProvider _time;

    internal JobServerHealthCheck(
        JobServer server, JobCircuitBreaker breakers, QueueMonitor queue, AbandonedJobs abandoned, IOptions<BackgroundJobsOptions> options,
        TimeProvider time)
    {
        _abandoned = abandoned;
        _server = server;
        _breakers = breakers;
        _queue = queue;
        _options = options;
        _time = time;
    }

    /// <summary>
    /// True once the server migrated the store, recovered interrupted jobs and is processing; false again once it stops.
    /// </summary>
    public bool IsReady => _server.IsReady;

    /// <summary>
    /// The full health picture; cheap (reads the latest queue sample, does not query the store).
    /// </summary>
    public JobServerHealthReport Report()
    {
        var threshold = _options.Value.StalledQueueThreshold;
        var sample = _queue.Latest;
        var queue = new QueueHealth(
            sample?.Ready, sample?.OldestReadyAge, sample?.At, sample != null && sample.OldestReadyAge > threshold, threshold,
            sample?.Queues.Select(q => new QueueDepthHealth(
                q.Queue, _options.Value.ConsumedQueues.ContainsKey(q.Queue), q.Ready, q.OldestReadyAge,
                !q.HasConsumer && q.Ready > 0 && q.OldestReadyAge > threshold)).ToList() ?? []);
        var breakers = _breakers.Snapshot()
            .Select(b => new CircuitBreakerHealth(b.Job, b.State, b.ConsecutiveFailures, b.OpenUntil)).ToList();
        var o = _options.Value;
        var now = _time.GetUtcNow();
        var jobs = _abandoned.Snapshot().Select(a => new AbandonedJobHealth(a.Id, a.Name, a.Since, now - a.Since)).ToList();
        var abandoned = new AbandonedHealth(jobs, o.MaxAbandonedJobs, o.MaxAbandonedJobs > 0 && jobs.Count >= o.MaxAbandonedJobs,
            jobs.Any(j => j.Age > o.AbandonedJobAlertAfter), o.AbandonedJobAlertAfter);
        var status = !IsReady || abandoned.Full ? HealthStatus.Unhealthy
            : queue.Stalled || queue.Queues.Any(q => q.Orphaned) || breakers.Any(b => b.State == "open") || abandoned.Alert ? HealthStatus.Degraded
            : HealthStatus.Healthy;
        return new JobServerHealthReport(status, IsReady, queue, breakers, abandoned, now);
    }

    /// <summary>
    /// <see cref="Report"/> as an ASP.NET health check result, with its main figures in the result data.
    /// </summary>
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
            ["abandoned"] = report.Abandoned.Jobs.Count,
        };
        return Task.FromResult(report.Status switch
        {
            HealthStatus.Unhealthy => HealthCheckResult.Unhealthy(report.Ready
                ? $"{report.Abandoned.Jobs.Count} jobs run without a worker, the MaxAbandonedJobs limit; a restart frees them"
                : "job server is not processing", data: data),
            HealthStatus.Degraded => HealthCheckResult.Degraded(Describe(report), data: data),
            _ => HealthCheckResult.Healthy(data: data),
        });
    }

    private static string Describe(JobServerHealthReport report) => string.Join("; ", new[]
    {
        report.Queue.Stalled ? $"queue stalled: oldest ready job waiting {report.Queue.OldestReadyAge!.Value.TotalSeconds:0}s" : null,
        report.Queue.Queues.Any(q => q.Orphaned)
            ? "queues no node consumes: " + string.Join(", ", report.Queue.Queues.Where(q => q.Orphaned).Select(q => q.Queue))
            : null,
        report.CircuitBreakers.Any(b => b.State == "open")
            ? "circuit breakers open: " + string.Join(", ", report.CircuitBreakers.Where(b => b.State == "open").Select(b => b.Job))
            : null,
        report.Abandoned.Alert
            ? $"jobs running without a worker for over {report.Abandoned.AlertAfter.TotalSeconds:0}s: " +
              string.Join(", ", report.Abandoned.Jobs.Where(j => j.Age > report.Abandoned.AlertAfter).Select(j => $"{j.Name} {j.Id}"))
            : null,
    }.Where(s => s != null));
}

/// <summary>
/// See <see cref="JobServerHealthCheck"/> for how <paramref name="Status"/> is derived.
/// </summary>
/// <param name="Status">Healthy, degraded or unhealthy.</param>
/// <param name="Ready">See <see cref="JobServerHealthCheck.IsReady"/>.</param>
/// <param name="Queue">Ready-queue depth and age.</param>
/// <param name="CircuitBreakers">One per job name that has a circuit breaker configured.</param>
/// <param name="Abandoned">Jobs running without a worker.</param>
/// <param name="At">When the report was built.</param>
public sealed record JobServerHealthReport(
    HealthStatus Status, bool Ready, QueueHealth Queue, IReadOnlyList<CircuitBreakerHealth> CircuitBreakers, AbandonedHealth Abandoned,
    DateTimeOffset At);

/// <summary>
/// Jobs still running past their timeout or cancel after giving their worker back (see BackgroundJobsOptions.MaxAbandonedJobs).
/// Full: the limit is reached. Alert: one has run that way for longer than AlertAfter.
/// </summary>
public sealed record AbandonedHealth(IReadOnlyList<AbandonedJobHealth> Jobs, int Limit, bool Full, bool Alert, TimeSpan AlertAfter);

/// <summary>
/// A job running past its timeout or cancel without a worker.
/// </summary>
/// <param name="Id">The job.</param>
/// <param name="Name">Its job name.</param>
/// <param name="Since">When it gave its worker back.</param>
/// <param name="Age">How long it has run that way.</param>
public sealed record AbandonedJobHealth(Guid Id, string Name, DateTimeOffset Since, TimeSpan Age);

/// <summary>
/// Ready/OldestReadyAge/SampledAt are null until the queues were sampled once. They, and Stalled, cover the queues this
/// node consumes: a queue served only by other nodes does not make this one unhealthy. Queues breaks them down per queue.
/// </summary>
public sealed record QueueHealth(
    long? Ready, TimeSpan? OldestReadyAge, DateTimeOffset? SampledAt, bool Stalled, TimeSpan StalledThreshold, IReadOnlyList<QueueDepthHealth> Queues);

/// <summary>
/// Consumed: whether this node has workers for the queue. Orphaned: no live node consumes it and its oldest ready job has
/// waited longer than the stalled threshold, e.g. a job enqueued into a misspelled queue.
/// </summary>
public sealed record QueueDepthHealth(string Queue, bool Consumed, long Ready, TimeSpan OldestReadyAge, bool Orphaned);

/// <summary>
/// State: closed, open or half-open.
/// </summary>
public sealed record CircuitBreakerHealth(string Job, string State, int ConsecutiveFailures, DateTimeOffset? OpenUntil);
