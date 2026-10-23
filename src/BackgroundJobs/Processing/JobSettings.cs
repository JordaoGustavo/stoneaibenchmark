namespace BackgroundJobs.Processing;

/// <summary>
/// What a created job runs with: per-job options resolved against the server defaults and validated. Shared by
/// enqueues (<see cref="JobOptions"/>) and recurring definitions (<see cref="RecurringJobOptions"/>).
/// </summary>
internal sealed record JobSettings(int MaxAttempts, int RetryDelayMs, int RetryJitterMs, string Queue, int Priority)
{
    /// <exception cref="ArgumentException"/>
    public static JobSettings Resolve(
        BackgroundJobsOptions defaults, int? maxAttempts, TimeSpan? retryDelay, TimeSpan? retryJitter, string? queue, int? priority)
    {
        var attempts = maxAttempts ?? defaults.DefaultMaxAttempts;
        var delay = retryDelay ?? defaults.DefaultRetryDelay;
        var jitter = retryJitter ?? defaults.DefaultRetryJitter;
        if (attempts < 1)
            throw new ArgumentException(
                $"MaxAttempts is {attempts}, but it must be at least 1 (it counts the first execution); fix the MaxAttempts option, " +
                "or BackgroundJobsOptions.DefaultMaxAttempts when the job does not set it");
        if (delay < TimeSpan.Zero || delay.TotalMilliseconds > int.MaxValue)
            throw new ArgumentException(
                $"RetryDelay is {delay}, but it must be from zero to {TimeSpan.FromMilliseconds(int.MaxValue)}; fix the RetryDelay option, " +
                "or BackgroundJobsOptions.DefaultRetryDelay when the job does not set it");
        if (jitter < TimeSpan.Zero || jitter.TotalMilliseconds > int.MaxValue)
            throw new ArgumentException(
                $"RetryJitter is {jitter}, but it must be from zero to {TimeSpan.FromMilliseconds(int.MaxValue)}; fix the RetryJitter option, " +
                "or BackgroundJobsOptions.DefaultRetryJitter when the job does not set it");
        return new JobSettings(attempts, (int)delay.TotalMilliseconds, (int)jitter.TotalMilliseconds,
            JobQueues.CheckName(queue ?? JobQueues.Default), JobQueues.CheckPriority(priority ?? JobQueues.DefaultPriority));
    }
}
