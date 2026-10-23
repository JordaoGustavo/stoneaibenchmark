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
    /// Npgsql connection string of the Postgres store. Required unless <see cref="UseInMemoryStorage"/> was called.
    public string ConnectionString { get; set; } = "";

    internal bool InMemory { get; private set; }

    /// Keeps jobs in this process instead of Postgres: for tests and local runs of code that integrates the library.
    /// Everything is lost when the process exits and nodes share nothing, so never use it in production.
    public BackgroundJobsOptions UseInMemoryStorage()
    {
        InMemory = true;
        return this;
    }

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

    /// How often the ready queue is sampled for metrics and the health check.
    public TimeSpan QueueMonitorInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// The health check reports a stalled queue when a job has been ready for longer than this.
    public TimeSpan StalledQueueThreshold { get; set; } = TimeSpan.FromSeconds(60);

    internal void Validate()
    {
        if (!InMemory && string.IsNullOrWhiteSpace(ConnectionString)) throw new ArgumentException("ConnectionString is required");
        if (WorkerCount < 1) throw new ArgumentException("WorkerCount must be >= 1");
        if (ClaimBatchSize < 1) throw new ArgumentException("ClaimBatchSize must be >= 1");
        if (LeaseDuration <= TimeSpan.Zero) throw new ArgumentException("LeaseDuration must be positive");
        if (QueueMonitorInterval <= TimeSpan.Zero) throw new ArgumentException("QueueMonitorInterval must be positive");
        if (StalledQueueThreshold <= TimeSpan.Zero) throw new ArgumentException("StalledQueueThreshold must be positive");
        if (SchedulerMaxWait <= TimeSpan.Zero) throw new ArgumentException("SchedulerMaxWait must be positive");
    }
}
