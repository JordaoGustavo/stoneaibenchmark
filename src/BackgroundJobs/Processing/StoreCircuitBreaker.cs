using Microsoft.Extensions.Logging;

namespace BackgroundJobs.Processing;

/// <summary>
/// Shared by every loop that touches the database: after a failure, all of them back off
/// exponentially (100 ms → 5 s) until a call succeeds again.
/// </summary>
internal sealed class StoreCircuitBreaker(TimeProvider time, ILogger<StoreCircuitBreaker> logger)
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);
    private readonly Lock _lock = new();
    private int _failures;
    private DateTimeOffset _openUntil;

    public async Task<T?> TryAsync<T>(string operation, Func<Task<T>> call, CancellationToken ct)
    {
        await WaitWhileOpenAsync(ct);
        try
        {
            var result = await call();
            lock (_lock) _failures = 0;
            return result;
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            TimeSpan backoff;
            lock (_lock)
            {
                _failures++;
                backoff = TimeSpan.FromMilliseconds(Math.Min(MaxBackoff.TotalMilliseconds, 100 * Math.Pow(2, _failures - 1)));
                _openUntil = time.GetUtcNow() + backoff;
            }
            logger.LogWarning(e, "store call {Operation} failed; backing off {Backoff}", operation, backoff);
            return default;
        }
    }

    private Task WaitWhileOpenAsync(CancellationToken ct)
    {
        TimeSpan wait;
        lock (_lock) wait = _openUntil - time.GetUtcNow();
        return wait > TimeSpan.Zero ? Task.Delay(wait, time, ct) : Task.CompletedTask;
    }
}
