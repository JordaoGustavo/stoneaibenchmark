namespace BackgroundJobs.Processing;

/// <summary>
/// The backoff a created job runs with, resolved against the server defaults and validated. Fixed is stored as
/// multiplier 1 with no cap, which computes the same delay as before backoff existed.
/// </summary>
internal sealed record BackoffSettings(double Multiplier, int? MaxDelayMs)
{
    public static readonly BackoffSettings Fixed = new(1, null);

    /// <exception cref="ArgumentException"/>
    public static BackoffSettings Resolve(BackgroundJobsOptions defaults, RetryBackoff? backoff, double? multiplier, TimeSpan? maxDelay)
    {
        var kind = backoff ?? defaults.DefaultRetryBackoff;
        if (!Enum.IsDefined(kind))
            throw new ArgumentException(
                $"RetryBackoff is {kind}, but it must be Fixed or Exponential; fix the RetryBackoff option, " +
                "or BackgroundJobsOptions.DefaultRetryBackoff when the job does not set it");
        var factor = multiplier ?? defaults.DefaultRetryBackoffMultiplier;
        var cap = maxDelay ?? defaults.DefaultMaxRetryDelay;
        if (!IsValidMultiplier(factor))
            throw new ArgumentException(
                $"RetryBackoffMultiplier is {factor}, but it must be a finite number from 1 to {MaxMultiplier}; fix the RetryBackoffMultiplier option, " +
                "or BackgroundJobsOptions.DefaultRetryBackoffMultiplier when the job does not set it");
        if (!IsValidMaxDelay(cap))
            throw new ArgumentException(
                $"MaxRetryDelay is {cap}, but it must be from zero to {TimeSpan.FromMilliseconds(int.MaxValue)}, or Timeout.InfiniteTimeSpan for no cap; " +
                "fix the MaxRetryDelay option, or BackgroundJobsOptions.DefaultMaxRetryDelay when the job does not set it");
        if (kind == RetryBackoff.Fixed) return Fixed;
        return new BackoffSettings(factor, cap == Timeout.InfiniteTimeSpan ? null : (int)cap.TotalMilliseconds);
    }

    public const double MaxMultiplier = 100;

    internal static bool IsValidMultiplier(double m) => double.IsFinite(m) && m >= 1 && m <= MaxMultiplier;

    internal static bool IsValidMaxDelay(TimeSpan t) =>
        t == Timeout.InfiniteTimeSpan || (t >= TimeSpan.Zero && t.TotalMilliseconds <= int.MaxValue);

    /// <summary>
    /// Delay before the retry that follows the <paramref name="countedAttempts"/>-th counted attempt, without jitter.
    /// </summary>
    public static long DelayMs(int retryDelayMs, double multiplier, int? maxDelayMs, int countedAttempts)
    {
        // multiplier == 1 is the fixed delay: exactly retryDelayMs, no cap (as before backoff existed).
        if (multiplier <= 1) return retryDelayMs;
        var delay = retryDelayMs * Math.Pow(multiplier, Math.Max(0, countedAttempts - 1));
        var cap = maxDelayMs ?? (double)int.MaxValue;
        return (long)Math.Min(delay, cap);
    }
}
