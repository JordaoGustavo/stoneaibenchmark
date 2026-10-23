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

    public JobsMetrics(IMeterFactory meters, QueueMonitor queue, AbandonedJobs abandoned)
    {
        var meter = meters.Create(BackgroundJobsTelemetry.Name);
        _duration = meter.CreateHistogram<double>("backgroundjobs.attempt.duration", "s", "Duration of one job attempt");
        // Failure rate = attempts{outcome=failed} / attempts.
        _attempts = meter.CreateCounter<long>("backgroundjobs.attempts", "{attempt}", "Finished job attempts by outcome");
        meter.CreateObservableGauge("backgroundjobs.queue.depth",
            () => queue.Latest?.Queues.Select(q => new Measurement<long>(q.Ready, new KeyValuePair<string, object?>("queue", q.Queue))) ?? [],
            "{job}", "Jobs in enqueued, ready to run, per queue");
        meter.CreateObservableGauge("backgroundjobs.queue.oldest_age",
            () => queue.Latest?.Queues.Select(q => new Measurement<double>(q.OldestReadyAge.TotalSeconds, new KeyValuePair<string, object?>("queue", q.Queue))) ?? [],
            "s", "Age of the oldest job in enqueued, per queue with ready jobs");
        meter.CreateObservableGauge("backgroundjobs.jobs.abandoned", () => (long)abandoned.Snapshot().Count,
            "{job}", "Jobs still running past their timeout or cancel without a worker");
    }

    public void AttemptFinished(string jobName, AttemptOutcome outcome, TimeSpan duration)
    {
        var tags = new TagList { { "job.name", jobName }, { "outcome", outcome.ToWire() } };
        _duration.Record(duration.TotalSeconds, tags);
        _attempts.Add(1, tags);
    }
}
