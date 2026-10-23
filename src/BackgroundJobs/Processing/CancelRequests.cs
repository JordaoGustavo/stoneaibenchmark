using System.Collections.Concurrent;

namespace BackgroundJobs.Processing;

/// <summary>
/// A job's cancel switch. <see cref="LeaseLost"/> tells a lost lease apart from a user cancel.
/// </summary>
internal sealed class RunCancellation : CancellationTokenSource
{
    private volatile bool _leaseLost;

    /// <summary>
    /// This node no longer owns the job: another node may be running it, so this run must stop and record nothing.
    /// </summary>
    public bool LeaseLost => _leaseLost;

    public void LoseLease()
    {
        _leaseLost = true;
        try { Cancel(); }
        catch (ObjectDisposedException) { }
    }
}

/// <summary>
/// The user-cancel switch of each job running in this process. Fed by IJobClient.CancelAsync when the job runs here,
/// and by the lease heartbeat for cancels requested on other nodes and for leases this node lost.
/// </summary>
internal sealed class CancelRequests
{
    private readonly ConcurrentDictionary<Guid, RunCancellation> _running = new();

    /// <summary>
    /// Disposed by the caller once the job's code has returned.
    /// </summary>
    public RunCancellation Register(Guid jobId)
    {
        var source = new RunCancellation();
        _running[jobId] = source;
        return source;
    }

    /// <summary>
    /// Removes only <paramref name="source"/>, never a later run's.
    /// </summary>
    public void Unregister(Guid jobId, RunCancellation source) => _running.TryRemove(new(jobId, source));

    public void Request(Guid jobId)
    {
        if (!_running.TryGetValue(jobId, out var source)) return;
        try { source.Cancel(); }
        catch (ObjectDisposedException) { }
    }

    public void LoseLease(Guid jobId)
    {
        if (_running.TryGetValue(jobId, out var source)) source.LoseLease();
    }
}
