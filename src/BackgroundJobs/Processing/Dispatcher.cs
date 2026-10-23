using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// One claim loop per consumed queue, each with its own worker slots: a queue claims at most as many jobs as it has free
/// slots (no prefetch), so at most its worker count of functions run at once and a busy queue never takes another's slots.
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
        await Task.WhenAll(options.Value.ConsumedQueues.Select(q => RunQueueAsync(q.Key, q.Value, ct)));

        Task[] running;
        lock (_lock) running = _running.Values.ToArray();
        await Task.WhenAll(running);
    }

    /// Returns after <paramref name="ct"/> is cancelled; running jobs are awaited by <see cref="RunAsync"/>.
    private async Task RunQueueAsync(string queue, int workers, CancellationToken ct)
    {
        var o = options.Value;
        var wake = signals.ForQueue(queue);
        // Not disposed: running jobs of this queue release it after this loop returned.
        var slots = new SemaphoreSlim(workers, workers);
        // Jobs claimed in priority order since the last FIFO claim (see BackgroundJobsOptions.FifoClaimEvery).
        var sinceFifo = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await slots.WaitAsync(ct);
                var free = 1;
                var max = Math.Min(o.ClaimBatchSize ?? workers, workers);
                while (free < max && slots.Wait(0)) free++;

                // Up to three claims fill the free slots: one job that may be a reserved half-open trial, the oldest job when
                // a FIFO claim is due, then the rest by priority. A claim that comes back short means nothing more is ready.
                // Never cancel a claim halfway: a committed claim must reach the executor.
                var jobs = new List<ClaimedJob>();
                var permit = jobBreaker.BeginClaim();
                try
                {
                    var full = true;
                    if (permit.Trials.Count > 0)
                        full = await ClaimAsync(ClaimOrder.Priority, 1, permit.Blocked);
                    var skipped = permit.Blocked.Concat(permit.Trials).ToList();
                    if (full && jobs.Count < free && o.FifoClaimEvery is { } every && sinceFifo >= every - 1)
                    {
                        full = await ClaimAsync(ClaimOrder.Fifo, 1, skipped);
                        if (full) sinceFifo = 0;
                    }
                    if (full && jobs.Count < free) await ClaimAsync(ClaimOrder.Priority, free - jobs.Count, skipped);
                }
                finally { jobBreaker.EndClaim(permit, jobs.Select(j => j.Name)); }

                async Task<bool> ClaimAsync(ClaimOrder order, int limit, IReadOnlyCollection<string> excluded)
                {
                    var claimed = await storeBreaker.TryAsync("claim", () =>
                    {
                        var now = time.UtcNow();
                        return store.ClaimAsync(
                            new ClaimRequest(queue, order, o.NodeId, now, now + o.LeaseDuration, limit, excluded), CancellationToken.None);
                    }, CancellationToken.None) ?? [];
                    jobs.AddRange(claimed);
                    if (order == ClaimOrder.Priority) sinceFifo += claimed.Count;
                    return claimed.Count == limit;
                }

                if (free > jobs.Count) slots.Release(free - jobs.Count);
                if (jobs.Count == 0)
                {
                    await wake.WaitAsync(o.PollInterval, ct);
                    continue;
                }

                foreach (var job in jobs)
                {
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
                            wake.Set();
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
                        wake.Set();
                        return true;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }
}
