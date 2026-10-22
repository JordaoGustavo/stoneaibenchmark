using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Keeps leases of running jobs alive and recovers jobs whose lease expired (e.g. their node died).
internal sealed class LeaseHeartbeat(
    IJobStore store, Dispatcher dispatcher, StoreCircuitBreaker storeBreaker, Signals signals,
    IOptions<BackgroundJobsOptions> options, TimeProvider time)
{
    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(o.LeaseDuration / 3, time, ct); }
            catch (OperationCanceledException) { return; }

            await storeBreaker.TryAsync("renew-leases", async () =>
            {
                await store.RenewLeasesAsync(o.NodeId, dispatcher.RunningIds(), time.UtcNow() + o.LeaseDuration, ct);
                return true;
            }, ct);
            var recovered = await storeBreaker.TryAsync("recover", () => store.RecoverAsync(time.UtcNow(), null, ct), ct);
            if (recovered > 0) signals.Dispatcher.Set();
        }
    }
}
