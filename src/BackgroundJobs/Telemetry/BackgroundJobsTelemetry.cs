using System.Diagnostics;
using System.Diagnostics.Metrics;
using BackgroundJobs.Processing;

namespace BackgroundJobs;

/// Names to subscribe to with OpenTelemetry: .AddSource(BackgroundJobsTelemetry.Name) and .AddMeter(BackgroundJobsTelemetry.Name).
public static class BackgroundJobsTelemetry
{
    public const string Name = "BackgroundJobs";

    internal static readonly ActivitySource Source = new(Name);
}

/// Attempt metrics are recorded as jobs finish; queue gauges read the latest <see cref="QueueMonitor"/> sample.
internal sealed class JobsMetrics
{
    private readonly Histogram<double> _duration;
    private readonly Counter<long> _attempts;

    public JobsMetrics(IMeterFactory meters, QueueMonitor queue)
    {
        var meter = meters.Create(BackgroundJobsTelemetry.Name);
        _duration = meter.CreateHistogram<double>("backgroundjobs.attempt.duration", "s", "Duration of one job attempt");
        // Failure rate = attempts{outcome=failed} / attempts.
        _attempts = meter.CreateCounter<long>("backgroundjobs.attempts", "{attempt}", "Finished job attempts by outcome");
        meter.CreateObservableGauge("backgroundjobs.queue.depth",
            () => queue.Latest is { } s ? [new Measurement<long>(s.Ready)] : Array.Empty<Measurement<long>>(),
            "{job}", "Jobs in enqueued, ready to run");
        meter.CreateObservableGauge("backgroundjobs.queue.oldest_age",
            () => queue.Latest is { } s ? [new Measurement<double>(s.OldestReadyAge.TotalSeconds)] : Array.Empty<Measurement<double>>(),
            "s", "Age of the oldest job in enqueued (0 when empty)");
    }

    public void AttemptFinished(string jobName, AttemptOutcome outcome, TimeSpan duration)
    {
        var tags = new TagList { { "job.name", jobName }, { "outcome", outcome.ToWire() } };
        _duration.Record(duration.TotalSeconds, tags);
        _attempts.Add(1, tags);
    }
}
