namespace BackgroundJobs;

/// <summary>
/// How the delay between retries grows. See <see cref="JobOptions.RetryBackoff"/>.
/// </summary>
public enum RetryBackoff
{
    /// <summary>
    /// Every retry waits RetryDelay (plus jitter). The default.
    /// </summary>
    Fixed = 0,

    /// <summary>
    /// The retry after the n-th counted attempt waits RetryDelay * RetryBackoffMultiplier^(n-1), capped at MaxRetryDelay,
    /// plus jitter.
    /// </summary>
    Exponential = 1,
}
