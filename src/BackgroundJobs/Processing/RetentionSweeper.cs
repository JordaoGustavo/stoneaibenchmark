using BackgroundJobs.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Every RetentionSweepInterval, purges finished jobs older than FinishedJobRetention, in batches.
/// Every node sweeps; stores make concurrent purges safe.
internal sealed class RetentionSweeper(
    IJobStore store, StoreCircuitBreaker storeBreaker, IOptions<BackgroundJobsOptions> options, TimeProvider time, ILogger<RetentionSweeper> logger)
{
    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        if (o.FinishedJobRetention is not { } retention || retention == Timeout.InfiniteTimeSpan) return;
        while (!ct.IsCancellationRequested)
        {
            var cutoff = time.UtcNow() - retention;
            var total = 0;
            int purged;
            do
            {
                // 0 when the store is unavailable: the breaker already logged it; the next sweep retries.
                purged = await storeBreaker.TryAsync("retention", () => store.PurgeFinishedAsync(cutoff, o.RetentionBatchSize, ct), ct);
                total += purged;
            } while (purged == o.RetentionBatchSize && !ct.IsCancellationRequested);
            if (total > 0) logger.LogInformation("purged {Count} finished jobs older than {Retention}", total, retention);

            try { await Task.Delay(o.RetentionSweepInterval, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }
}
