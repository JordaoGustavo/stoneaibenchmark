using BackgroundJobs.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// <summary>
/// Keeps leases of running jobs alive and recovers jobs whose lease expired (e.g. their node died). Runs until the
/// dispatcher has finished every running job (the server stops it then), so a draining job keeps its lease.
/// A job whose lease this node may no longer hold is stopped locally: another node may already be running it.
/// </summary>
internal sealed class LeaseHeartbeat(
    IJobStore store, Dispatcher dispatcher, CancelRequests cancels, StoreCircuitBreaker storeBreaker, Signals signals,
    IOptions<BackgroundJobsOptions> options, TimeProvider time, ILogger<LeaseHeartbeat> logger)
{
    /// <summary>
    /// When each running job's lease was last known to be held: the timestamp taken just before the claim or renewal
    /// the store confirmed. Read on this node's monotonic clock, so it holds whatever the store's clock says.
    /// </summary>
    private readonly Dictionary<Guid, long> _confirmed = [];
    /// <summary>
    /// Running jobs already stopped for a lost lease (one that ignores its token keeps running a while).
    /// </summary>
    private readonly HashSet<Guid> _lost = [];
    private readonly Lock _lock = new();

    public Task RunAsync(CancellationToken ct) => Task.WhenAll(RenewAsync(ct), WatchAsync(ct));

    private async Task RenewAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(o.LeaseDuration / 3, time, ct); }
            catch (OperationCanceledException) { return; }

            var running = dispatcher.RunningLeases();
            var sentAt = time.GetTimestamp();
            var renewal = await storeBreaker.TryAsync("renew-leases",
                () => store.RenewLeasesAsync(o.NodeId, running.Keys.ToList(), time.UtcNow(), o.LeaseDuration, ct), ct);
            if (renewal is not null)
            {
                foreach (var id in renewal.CancelRequested) cancels.Request(id);
                // Not renewed though running since before the call: recovered, and maybe claimed by another node.
                var renewed = renewal.Renewed.ToHashSet();
                lock (_lock) foreach (var id in renewed) _confirmed[id] = sentAt;
                foreach (var id in running.Keys.Where(id => !renewed.Contains(id))) Lose(id);
            }
            await storeBreaker.TryAsync("register-consumer", () => RegisterAsync(store, o, time, ct), ct);
            var recovered = await storeBreaker.TryAsync("recover", () => store.RecoverAsync(time.UtcNow(), ct), ct);
            if (recovered > 0) signals.JobsReady();
        }
    }

    /// <summary>
    /// Independent of the store calls, which may hang or back off while the store is unreachable: a job whose last
    /// confirmed lease has run out may already have been recovered by another node, so it is stopped here.
    /// </summary>
    private async Task WatchAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(o.LeaseDuration / 10, time, ct); }
            catch (OperationCanceledException) { return; }

            var running = dispatcher.RunningLeases();
            List<Guid> expired;
            lock (_lock)
            {
                foreach (var id in _confirmed.Keys.Except(running.Keys).ToList()) _confirmed.Remove(id);
                _lost.IntersectWith(running.Keys);
                expired = running
                    .Where(r => time.GetElapsedTime(Math.Max(r.Value, _confirmed.GetValueOrDefault(r.Key))) >= o.LeaseDuration)
                    .Select(r => r.Key).ToList();
            }
            foreach (var id in expired) Lose(id);
        }
    }

    private void Lose(Guid id)
    {
        lock (_lock)
            if (!_lost.Add(id)) return;
        logger.LogWarning("job {JobId} lost its lease on this node; stopping it", id);
        cancels.LoseLease(id);
    }

    /// <summary>
    /// Tells the store which queues this node consumes, for as long as a lease: the heartbeat renews it every
    /// LeaseDuration / 3, so it lapses about as soon as a dead node's jobs are recovered.
    /// </summary>
    public static async Task<bool> RegisterAsync(IJobStore store, BackgroundJobsOptions o, TimeProvider time, CancellationToken ct)
    {
        var now = time.UtcNow();
        await store.RegisterConsumerAsync(o.NodeId, o.ConsumedQueues.Keys.ToList(), now, now + o.LeaseDuration, ct);
        return true;
    }
}
