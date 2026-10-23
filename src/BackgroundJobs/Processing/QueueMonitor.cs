using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// HasConsumer: some live node, this one or another, consumes the queue.
internal sealed record QueueDepth(string Queue, long Ready, TimeSpan OldestReadyAge, bool HasConsumer);

/// Ready/OldestReadyAge cover the queues this node consumes, the ones its health depends on. Queues lists those (empty
/// ones at 0) and every other queue with ready jobs.
internal sealed record QueueSample(long Ready, TimeSpan OldestReadyAge, DateTimeOffset At, IReadOnlyList<QueueDepth> Queues);

/// Samples the ready queues every QueueMonitorInterval for the queue gauges and the health check.
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
            var stats = await storeBreaker.TryAsync("queue-stats", () => store.QueueStatsAsync(time.UtcNow(), ct), ct);
            if (stats != null)
            {
                var now = time.UtcNow();
                var consumedNames = options.Value.ConsumedQueues.Keys;
                var queues = stats
                    .Select(s => new QueueDepth(
                        s.Queue, s.Ready, s.OldestReadyAt is { } oldest && oldest < now ? now - oldest : TimeSpan.Zero,
                        s.HasConsumer || consumedNames.Contains(s.Queue)))
                    .Concat(consumedNames.Where(q => stats.All(s => s.Queue != q)).Select(q => new QueueDepth(q, 0, TimeSpan.Zero, true)))
                    .OrderBy(q => q.Queue, StringComparer.Ordinal).ToList();
                var consumed = queues.Where(q => consumedNames.Contains(q.Queue)).ToList();
                _latest = new QueueSample(consumed.Sum(q => q.Ready), consumed.Select(q => q.OldestReadyAge).DefaultIfEmpty().Max(), now, queues);
            }
            try { await Task.Delay(options.Value.QueueMonitorInterval, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }
}
