namespace BackgroundJobs.Processing;

internal static class Clock
{
    /// UTC now truncated to Postgres' microsecond precision, so the value we compute with is the one we store.
    public static DateTime UtcNow(this TimeProvider time)
    {
        var now = time.GetUtcNow().UtcDateTime;
        return new DateTime(now.Ticks - now.Ticks % 10, DateTimeKind.Utc);
    }
}

/// Auto-reset wake-up: Set() releases at most one pending or future WaitAsync.
internal sealed class WakeSignal
{
    private readonly SemaphoreSlim _semaphore = new(0, 1);

    public void Set()
    {
        try { _semaphore.Release(); }
        catch (SemaphoreFullException) { }
    }

    public Task WaitAsync(TimeSpan timeout, CancellationToken ct) => _semaphore.WaitAsync(timeout, ct);
}

/// Separate signals so a wake-up for the dispatcher is never consumed by the scheduler.
internal sealed class Signals
{
    public WakeSignal Dispatcher { get; } = new();
}
