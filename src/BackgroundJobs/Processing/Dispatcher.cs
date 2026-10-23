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
    // Claims of all queues, one at a time: a claim and its OnClaimed must not interleave with another queue's, or two
    // queues could each take the half-open trial of the same job type.
    private readonly SemaphoreSlim _claimGate = new(1, 1);

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
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await slots.WaitAsync(ct);
                var free = 1;
                var max = Math.Min(o.ClaimBatchSize ?? workers, workers);
                while (free < max && slots.Wait(0)) free++;

                // Never cancel a claim halfway: a committed claim must reach the executor.
                IReadOnlyList<ClaimedJob> jobs;
                await _claimGate.WaitAsync(CancellationToken.None);
                try
                {
                    jobs = await storeBreaker.TryAsync("claim", () =>
                    {
                        var now = time.UtcNow();
                        var blocked = jobBreaker.BlockedNames();
                        var limit = jobBreaker.TrialPending() ? 1 : free;
                        return store.ClaimAsync(queue, o.NodeId, now, now + o.LeaseDuration, limit, blocked, CancellationToken.None);
                    }, CancellationToken.None) ?? [];
                    foreach (var job in jobs) jobBreaker.OnClaimed(job.Name);
                }
                finally { _claimGate.Release(); }

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
