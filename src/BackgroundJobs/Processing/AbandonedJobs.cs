namespace BackgroundJobs.Processing;

internal sealed record AbandonedJob(Guid Id, string Name, DateTimeOffset Since);

/// <summary>
/// Jobs of this process still running past their timeout or cancel after giving their worker back. Its own service so
/// the dispatcher, metrics and health check can share it without depending on each other.
/// </summary>
internal sealed class AbandonedJobs
{
    private readonly Dictionary<Guid, AbandonedJob> _jobs = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// False when <paramref name="limit"/> jobs are already abandoned.
    /// </summary>
    public bool TryAdd(AbandonedJob job, int limit)
    {
        lock (_lock)
        {
            if (_jobs.Count >= limit) return false;
            _jobs[job.Id] = job;
            return true;
        }
    }

    public void Remove(Guid id)
    {
        lock (_lock) _jobs.Remove(id);
    }

    /// <summary>
    /// Oldest first.
    /// </summary>
    public IReadOnlyList<AbandonedJob> Snapshot()
    {
        lock (_lock) return _jobs.Values.OrderBy(j => j.Since).ToList();
    }
}
