using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

internal static class Clock
{
    /// <summary>
    /// UTC now truncated to Postgres' microsecond precision, so the value we compute with is the one we store.
    /// </summary>
    public static DateTime UtcNow(this TimeProvider time)
    {
        var now = time.GetUtcNow().UtcDateTime;
        return new DateTime(now.Ticks - now.Ticks % 10, DateTimeKind.Utc);
    }
}

/// <summary>
/// Auto-reset wake-up: Set() releases at most one pending or future WaitAsync.
/// </summary>
internal sealed class WakeSignal
{
    private readonly SemaphoreSlim _semaphore = new(0, 1);

    public void Set()
    {
        try { _semaphore.Release(); }
        catch (SemaphoreFullException) { }
    }

    public Task WaitAsync(TimeSpan timeout, CancellationToken ct) => _semaphore.WaitAsync(timeout, ct);

    /// <summary>
    /// Like WaitAsync, but the timeout runs on <paramref name="time"/> so fake clocks drive it.
    /// </summary>
    public async Task WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var wake = _semaphore.WaitAsync(Timeout.Infinite, cts.Token);
        var delay = Task.Delay(timeout, time, cts.Token);
        await Task.WhenAny(wake, delay);
        await cts.CancelAsync();
        try { await Task.WhenAll(wake, delay); }
        catch (OperationCanceledException) { }
        ct.ThrowIfCancellationRequested();
    }
}

/// <summary>
/// Separate signals so a wake-up for the dispatcher is never consumed by the scheduler, and one per consumed queue so a
/// wake-up for one queue's loop is never consumed by another's. Wake-ups do not say which queue: every loop checks its own.
/// </summary>
internal sealed class Signals(IOptions<BackgroundJobsOptions> options) : Storage.IJobSignals
{
    private readonly Dictionary<string, WakeSignal> _queues = options.Value.ConsumedQueues.Keys.ToDictionary(q => q, _ => new WakeSignal());

    public WakeSignal ForQueue(string queue) => _queues[queue];
    public WakeSignal Scheduler { get; } = new();

    public void JobsReady()
    {
        foreach (var signal in _queues.Values) signal.Set();
    }

    public void SchedulesChanged() => Scheduler.Set();
}
