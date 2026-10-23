using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

internal sealed record QueueSample(long Ready, TimeSpan OldestReadyAge, DateTimeOffset At);

/// Samples the ready queue every QueueMonitorInterval for the queue gauges and the health check.
/// Ages are measured at sampling time, so they may lag by up to one interval.
internal sealed class QueueMonitor(
    IJobStore store, StoreCircuitBreaker storeBreaker, IOptions<BackgroundJobsOptions> options, TimeProvider time)
{
    private volatile QueueSample? _latest;

    /// Null until the first successful sample.
    public QueueSample? Latest => _latest;

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var stats = await storeBreaker.TryAsync("queue-stats", () => store.QueueStatsAsync(ct), ct);
            if (stats != null)
            {
                var now = time.UtcNow();
                var age = stats.OldestReadyAt is { } oldest && oldest < now ? now - oldest : TimeSpan.Zero;
                _latest = new QueueSample(stats.Ready, age, now);
            }
            try { await Task.Delay(options.Value.QueueMonitorInterval, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }
}
