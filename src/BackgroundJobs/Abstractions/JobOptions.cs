namespace BackgroundJobs;

/// Per-job options. Unset fields fall back to the defaults in <see cref="BackgroundJobsOptions"/>.
public sealed record JobOptions
{
    /// Not before this instant. Null or in the past means "enqueue now".
    public DateTimeOffset? RunAt { get; init; }

    /// Total executions allowed, including the first one.
    public int? MaxAttempts { get; init; }

    public TimeSpan? RetryDelay { get; init; }

    /// Random extra delay in [0, RetryJitter] added to each retry.
    public TimeSpan? RetryJitter { get; init; }
}

public sealed class BackgroundJobsOptions
{
    public int WorkerCount { get; set; } = 4;

    /// Identifies this process as lease owner. Restarting with the same id recovers its jobs immediately.
    public string NodeId { get; set; } = Environment.MachineName;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// Longest the scheduler sleeps without checking the store; a fallback for missed notifications.
    public TimeSpan SchedulerMaxWait { get; set; } = TimeSpan.FromSeconds(5);

    /// Max jobs claimed per query. Null: as many as there are free worker slots.
    public int? ClaimBatchSize { get; set; }

    public int DefaultMaxAttempts { get; set; } = 3;
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan DefaultRetryJitter { get; set; } = TimeSpan.Zero;

    /// Longest an attempt may run before its <see cref="JobContext.CancellationToken"/> fires and the attempt fails.
    /// <see cref="JobRegistrationOptions.Timeout"/> overrides it per job name. Infinite by default.
    public TimeSpan DefaultJobTimeout { get; set; } = Timeout.InfiniteTimeSpan;

    /// After a timeout, how long a job that ignores its token is waited for. Then the attempt is recorded as failed and its
    /// worker freed, while the job's code may still be running in the background.
    public TimeSpan TimeoutGracePeriod { get; set; } = TimeSpan.FromSeconds(5);

    /// How often the ready queue is sampled for metrics and the health check.
    public TimeSpan QueueMonitorInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// The health check reports a stalled queue when a job has been ready for longer than this.
    public TimeSpan StalledQueueThreshold { get; set; } = TimeSpan.FromSeconds(60);

    /// Required: how long a finished job (succeeded, failed or deleted) is kept, with its history and attempts, after it
    /// finished. Older ones are purged. <see cref="Timeout.InfiniteTimeSpan"/> keeps them forever (the tables then grow
    /// without bound). There is no default: how much history to keep is a decision of the application.
    public TimeSpan? FinishedJobRetention { get; set; }

    /// How often finished jobs past their retention are purged.
    public TimeSpan RetentionSweepInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// Max jobs purged per store call; a sweep repeats until fewer remain.
    public int RetentionBatchSize { get; set; } = 1000;

    internal void Validate()
    {
        if (FinishedJobRetention == null)
            throw new ArgumentException("FinishedJobRetention must be set: how long finished jobs are kept (Timeout.InfiniteTimeSpan keeps them forever)");
        if (FinishedJobRetention <= TimeSpan.Zero && FinishedJobRetention != Timeout.InfiniteTimeSpan)
            throw new ArgumentException("FinishedJobRetention must be positive or Timeout.InfiniteTimeSpan");
        if (RetentionSweepInterval <= TimeSpan.Zero) throw new ArgumentException("RetentionSweepInterval must be positive");
        if (RetentionBatchSize < 1) throw new ArgumentException("RetentionBatchSize must be >= 1");
        if (WorkerCount < 1) throw new ArgumentException("WorkerCount must be >= 1");
        if (ClaimBatchSize < 1) throw new ArgumentException("ClaimBatchSize must be >= 1");
        if (LeaseDuration <= TimeSpan.Zero) throw new ArgumentException("LeaseDuration must be positive");
        if (QueueMonitorInterval <= TimeSpan.Zero) throw new ArgumentException("QueueMonitorInterval must be positive");
        if (StalledQueueThreshold <= TimeSpan.Zero) throw new ArgumentException("StalledQueueThreshold must be positive");
        if (!IsValidTimeout(DefaultJobTimeout))
            throw new ArgumentException("DefaultJobTimeout must be positive or Timeout.InfiniteTimeSpan");
        if (TimeoutGracePeriod < TimeSpan.Zero) throw new ArgumentException("TimeoutGracePeriod must not be negative");
        if (SchedulerMaxWait <= TimeSpan.Zero) throw new ArgumentException("SchedulerMaxWait must be positive");
    }

    internal static bool IsValidTimeout(TimeSpan t) => t > TimeSpan.Zero || t == Timeout.InfiniteTimeSpan;
}
