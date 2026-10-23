namespace BackgroundJobs.Processing;

/// What a created job runs with: per-job options resolved against the server defaults and validated. Shared by
/// enqueues (<see cref="JobOptions"/>) and recurring definitions (<see cref="RecurringJobOptions"/>).
internal sealed record JobSettings(int MaxAttempts, int RetryDelayMs, int RetryJitterMs, string Queue, int Priority)
{
    /// <exception cref="ArgumentException"/>
    public static JobSettings Resolve(
        BackgroundJobsOptions defaults, int? maxAttempts, TimeSpan? retryDelay, TimeSpan? retryJitter, string? queue, int? priority)
    {
        var attempts = maxAttempts ?? defaults.DefaultMaxAttempts;
        var delay = retryDelay ?? defaults.DefaultRetryDelay;
        var jitter = retryJitter ?? defaults.DefaultRetryJitter;
        if (attempts < 1) throw new ArgumentException("maxAttempts must be >= 1");
        if (delay < TimeSpan.Zero || delay.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryDelay is out of range");
        if (jitter < TimeSpan.Zero || jitter.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryJitter is out of range");
        return new JobSettings(attempts, (int)delay.TotalMilliseconds, (int)jitter.TotalMilliseconds,
            JobQueues.CheckName(queue ?? JobQueues.Default), JobQueues.CheckPriority(priority ?? JobQueues.DefaultPriority));
    }
}
