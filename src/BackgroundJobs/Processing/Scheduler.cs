using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Promotes due scheduled jobs (new schedules and retries) to enqueued. Sleeps until the next run_at
/// (capped by SchedulerMaxWait) and wakes early when a job gets scheduled.
internal sealed class Scheduler(
    IJobStore store, StoreCircuitBreaker storeBreaker, Signals signals, IOptions<BackgroundJobsOptions> options, TimeProvider time)
{
    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            var promoted = await storeBreaker.TryAsync("promote", () => store.PromoteDueAsync(time.UtcNow(), ct), ct);
            if (promoted > 0) signals.Dispatcher.Set();
            // Boxed so a store failure (null) is distinguishable from "nothing scheduled".
            var next = await storeBreaker.TryAsync("next-run-at", async () => Tuple.Create(await store.NextRunAtAsync(ct)), ct);
            var wait = next is null ? o.PollInterval
                : next.Item1 is { } at ? Min(at - time.UtcNow(), o.SchedulerMaxWait)
                : o.SchedulerMaxWait;
            if (wait <= TimeSpan.Zero) continue;
            try { await signals.Scheduler.WaitAsync(wait, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
