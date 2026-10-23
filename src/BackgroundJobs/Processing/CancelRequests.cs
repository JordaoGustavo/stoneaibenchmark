using System.Collections.Concurrent;

namespace BackgroundJobs.Processing;

/// The user-cancel switch of each job running in this process. Fed by IJobClient.CancelAsync when the job runs here,
/// and by the lease heartbeat for cancels requested on other nodes.
internal sealed class CancelRequests
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _running = new();

    /// Disposed by the caller once the job's code has returned.
    public CancellationTokenSource Register(Guid jobId)
    {
        var source = new CancellationTokenSource();
        _running[jobId] = source;
        return source;
    }

    /// Removes only <paramref name="source"/>, never a later run's.
    public void Unregister(Guid jobId, CancellationTokenSource source) => _running.TryRemove(new(jobId, source));

    public void Request(Guid jobId)
    {
        if (!_running.TryGetValue(jobId, out var source)) return;
        try { source.Cancel(); }
        catch (ObjectDisposedException) { }
    }
}
