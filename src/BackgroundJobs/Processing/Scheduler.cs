using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Promotes due scheduled jobs (new schedules and retries) to enqueued.
internal sealed class Scheduler(
    IJobStore store, StoreCircuitBreaker storeBreaker, Signals signals, IOptions<BackgroundJobsOptions> options, TimeProvider time)
{
    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var promoted = await storeBreaker.TryAsync("promote", () => store.PromoteDueAsync(time.UtcNow(), ct), ct);
            if (promoted > 0) signals.Dispatcher.Set();
            try { await Task.Delay(options.Value.PollInterval, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }
}
