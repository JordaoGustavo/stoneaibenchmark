using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Keeps leases of running jobs alive and recovers jobs whose lease expired (e.g. their node died).
internal sealed class LeaseHeartbeat(
    IJobStore store, Dispatcher dispatcher, CancelRequests cancels, StoreCircuitBreaker storeBreaker, Signals signals,
    IOptions<BackgroundJobsOptions> options, TimeProvider time)
{
    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(o.LeaseDuration / 3, time, ct); }
            catch (OperationCanceledException) { return; }

            var cancelRequested = await storeBreaker.TryAsync("renew-leases",
                () => store.RenewLeasesAsync(o.NodeId, dispatcher.RunningIds(), time.UtcNow() + o.LeaseDuration, ct), ct);
            foreach (var id in cancelRequested ?? []) cancels.Request(id);
            await storeBreaker.TryAsync("register-consumer", () => RegisterAsync(store, o, time, ct), ct);
            var recovered = await storeBreaker.TryAsync("recover", () => store.RecoverAsync(time.UtcNow(), ct), ct);
            if (recovered > 0) signals.JobsReady();
        }
    }

    /// Tells the store which queues this node consumes, for as long as a lease: the heartbeat renews it every
    /// LeaseDuration / 3, so it lapses about as soon as a dead node's jobs are recovered.
    public static async Task<bool> RegisterAsync(IJobStore store, BackgroundJobsOptions o, TimeProvider time, CancellationToken ct)
    {
        var now = time.UtcNow();
        await store.RegisterConsumerAsync(o.NodeId, o.ConsumedQueues.Keys.ToList(), now, now + o.LeaseDuration, ct);
        return true;
    }
}
