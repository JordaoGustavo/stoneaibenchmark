using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Claims at most as many jobs as there are free worker slots (no prefetch), so at most WorkerCount functions run at once.
internal sealed class Dispatcher(
    IJobStore store,
    JobExecutor executor,
    AbandonedJobs abandonedJobs,
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
                var free = 1;
                var max = Math.Min(o.ClaimBatchSize ?? o.WorkerCount, o.WorkerCount);
                while (free < max && slots.Wait(0)) free++;

                // Never cancel a claim halfway: a committed claim must reach the executor.
                var jobs = await storeBreaker.TryAsync("claim", () =>
                {
                    var now = time.UtcNow();
                    var blocked = jobBreaker.BlockedNames();
                    var limit = jobBreaker.TrialPending() ? 1 : free;
                    return store.ClaimAsync(o.NodeId, now, now + o.LeaseDuration, limit, blocked, CancellationToken.None);
                }, CancellationToken.None) ?? [];

                if (free > jobs.Count) slots.Release(free - jobs.Count);
                if (jobs.Count == 0)
                {
                    await signals.Dispatcher.WaitAsync(o.PollInterval, ct);
                    continue;
                }

                foreach (var job in jobs)
                {
                    jobBreaker.OnClaimed(job.Name);
                    var abandoned = false;
                    lock (_lock) _running[job.Id] = Task.Run(async () =>
                    {
                        try { await executor.ExecuteAsync(job, ct, Abandon); }
                        finally
                        {
                            lock (_lock)
                            {
                                _running.Remove(job.Id);
                                if (abandoned) abandonedJobs.Remove(job.Id);
                            }
                            if (!abandoned) slots.Release();
                            signals.Dispatcher.Set();
                        }
                    }, CancellationToken.None);

                    // Gives the job's worker to the next job while it keeps running (and stays in RunningIds, so its lease
                    // is renewed). False when MaxAbandonedJobs are already running that way.
                    bool Abandon()
                    {
                        lock (_lock)
                        {
                            if (abandoned || !abandonedJobs.TryAdd(new AbandonedJob(job.Id, job.Name, time.GetUtcNow()), o.MaxAbandonedJobs))
                                return false;
                            abandoned = true;
                        }
                        slots.Release();
                        signals.Dispatcher.Set();
                        return true;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }

        Task[] running;
        lock (_lock) running = _running.Values.ToArray();
        await Task.WhenAll(running);
    }
}
