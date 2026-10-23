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
    public string ConnectionString { get; set; } = "";
    public int WorkerCount { get; set; } = 4;

    /// Identifies this process as lease owner. Restarting with the same id recovers its jobs immediately.
    public string NodeId { get; set; } = Environment.MachineName;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// Max jobs claimed per query. Null: as many as there are free worker slots.
    public int? ClaimBatchSize { get; set; }

    public int DefaultMaxAttempts { get; set; } = 3;
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan DefaultRetryJitter { get; set; } = TimeSpan.Zero;

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) throw new ArgumentException("ConnectionString is required");
        if (WorkerCount < 1) throw new ArgumentException("WorkerCount must be >= 1");
        if (ClaimBatchSize < 1) throw new ArgumentException("ClaimBatchSize must be >= 1");
        if (LeaseDuration <= TimeSpan.Zero) throw new ArgumentException("LeaseDuration must be positive");
    }
}
