using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Claims a job only when a worker slot is free (no prefetch), so at most WorkerCount functions run at once.
internal sealed class Dispatcher(
    IJobStore store,
    JobExecutor executor,
    JobCircuitBreaker jobBreaker,
    StoreCircuitBreaker storeBreaker,
    Signals signals,
    IOptions<BackgroundJobsOptions> options,
    TimeProvider time)
{
    private readonly Dictionary<Guid, Task> _running = [];
    private readonly Lock _lock = new();

    public IReadOnlyCollection<Guid> RunningIds()
    {
        lock (_lock) return _running.Keys.ToList();
    }

    /// Returns after <paramref name="ct"/> is cancelled and every running job has finished.
    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        using var slots = new SemaphoreSlim(o.WorkerCount, o.WorkerCount);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await slots.WaitAsync(ct);
                // Never cancel a claim halfway: a committed claim must reach the executor.
                var job = await storeBreaker.TryAsync("claim", () =>
                {
                    var now = time.UtcNow();
                    return store.ClaimAsync(o.NodeId, now, now + o.LeaseDuration, jobBreaker.BlockedNames(), CancellationToken.None);
                }, CancellationToken.None);

                if (job == null)
                {
                    slots.Release();
                    await signals.Dispatcher.WaitAsync(o.PollInterval, ct);
                    continue;
                }

                jobBreaker.OnClaimed(job.Name);
                lock (_lock) _running[job.Id] = Task.Run(async () =>
                {
                    try { await executor.ExecuteAsync(job); }
                    finally
                    {
                        lock (_lock) _running.Remove(job.Id);
                        slots.Release();
                        signals.Dispatcher.Set();
                    }
                }, CancellationToken.None);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }

        Task[] running;
        lock (_lock) running = _running.Values.ToArray();
        await Task.WhenAll(running);
    }
}
