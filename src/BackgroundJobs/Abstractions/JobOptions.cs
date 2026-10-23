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

    /// The queue the job waits in; only servers consuming it (<see cref="BackgroundJobsOptions.Queues"/>) run it.
    /// Defaults to <see cref="JobQueues.Default"/>. See <see cref="JobQueues.IsValidName"/>.
    public string? Queue { get; init; }

    /// Higher runs first within its queue, between <see cref="JobQueues.MinPriority"/> and <see cref="JobQueues.MaxPriority"/>.
    /// Defaults to <see cref="JobQueues.DefaultPriority"/>.
    public int? Priority { get; init; }

    /// While a job enqueued with this key is not finished (scheduled, enqueued or processing), enqueueing the key again
    /// creates nothing and returns that job's id, whatever the new name, args and options. Once it finished, the key is free. Up to <see cref="MaxIdempotencyKeyLength"/>
    /// characters, not blank. Null: no deduplication.
    public string? IdempotencyKey { get; init; }

    public const int MaxIdempotencyKeyLength = 200;

    internal static void CheckIdempotencyKey(string? key)
    {
        if (key == null) return;
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentException("idempotency key must not be blank");
        if (key.Length > MaxIdempotencyKeyLength)
            throw new ArgumentException($"idempotency key must be at most {MaxIdempotencyKeyLength} characters");
    }
}

public sealed class BackgroundJobsOptions
{
    /// Workers for <see cref="JobQueues.Default"/> when <see cref="Queues"/> is empty; ignored otherwise.
    public int WorkerCount { get; set; } = 4;

    /// The queues this server consumes and how many workers each one gets; each queue has its own workers, so a busy
    /// queue never holds up another. Empty: only <see cref="JobQueues.Default"/>, with <see cref="WorkerCount"/> workers.
    /// Jobs in a queue no server consumes stay enqueued.
    public Dictionary<string, int> Queues { get; set; } = [];

    /// <see cref="Queues"/>, or the default queue with <see cref="WorkerCount"/> workers when it is empty.
    internal IReadOnlyDictionary<string, int> ConsumedQueues =>
        Queues.Count > 0 ? Queues : new Dictionary<string, int> { [JobQueues.Default] = WorkerCount };

    internal int TotalWorkers => ConsumedQueues.Values.Sum();

    /// Identifies this process as lease owner. Restarting with the same id recovers its jobs immediately.
    public string NodeId { get; set; } = Environment.MachineName;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// Longest the scheduler sleeps without checking the store; a fallback for missed notifications.
    public TimeSpan SchedulerMaxWait { get; set; } = TimeSpan.FromSeconds(5);

    /// Every this many jobs claimed from a queue, one is its oldest ready job whatever its priority, so a steady stream of
    /// high-priority jobs cannot starve the low ones: they get at least 1/FifoClaimEvery of the claims. Null: strict
    /// priority. Must be >= 2.
    public int? FifoClaimEvery { get; set; } = 10;

    /// Max jobs claimed per query, per queue. Null: as many as the queue has free worker slots.
    public int? ClaimBatchSize { get; set; }

    public int DefaultMaxAttempts { get; set; } = 3;
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan DefaultRetryJitter { get; set; } = TimeSpan.Zero;

    /// Longest an attempt may run before its <see cref="JobContext.CancellationToken"/> fires and the attempt fails.
    /// <see cref="JobRegistrationOptions.Timeout"/> overrides it per job name. Infinite by default.
    public TimeSpan DefaultJobTimeout { get; set; } = Timeout.InfiniteTimeSpan;

    /// After a timeout or a cancel, how long a job that ignores its token keeps its worker. Then the worker is freed for
    /// other jobs while the job's code keeps running; the job stays processing (its lease renewed, so it never runs twice)
    /// until the code returns, and that result is recorded.
    public TimeSpan TimeoutGracePeriod { get; set; } = TimeSpan.FromSeconds(5);

    /// Most jobs that may run past their grace period at once without a worker. Beyond it, such a job keeps its worker.
    /// Bounds the threads a pile of stuck jobs can take: at most (all queues' workers) + MaxAbandonedJobs jobs run at once.
    public int MaxAbandonedJobs { get; set; } = 4;

    /// The health check reports degraded once a job has run without its worker for longer than this. A job whose code
    /// never returns stays processing for good, so this is how it gets noticed.
    public TimeSpan AbandonedJobAlertAfter { get; set; } = TimeSpan.FromMinutes(5);

    /// When the server stops, how long running jobs are waited for after their token fired. Jobs still running then are
    /// recorded as interrupted and requeued (their attempt given back), and the server stops. Infinite by default.
    public TimeSpan ShutdownTimeout { get; set; } = Timeout.InfiniteTimeSpan;

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
        foreach (var (queue, workers) in Queues)
        {
            JobQueues.CheckName(queue);
            if (workers < 1) throw new ArgumentException($"Queues['{queue}'] must be >= 1");
        }
        if (ClaimBatchSize < 1) throw new ArgumentException("ClaimBatchSize must be >= 1");
        if (FifoClaimEvery < 2) throw new ArgumentException("FifoClaimEvery must be >= 2, or null for strict priority");
        if (LeaseDuration <= TimeSpan.Zero) throw new ArgumentException("LeaseDuration must be positive");
        if (QueueMonitorInterval <= TimeSpan.Zero) throw new ArgumentException("QueueMonitorInterval must be positive");
        if (StalledQueueThreshold <= TimeSpan.Zero) throw new ArgumentException("StalledQueueThreshold must be positive");
        if (!IsValidTimeout(DefaultJobTimeout))
            throw new ArgumentException("DefaultJobTimeout must be positive or Timeout.InfiniteTimeSpan");
        if (TimeoutGracePeriod < TimeSpan.Zero) throw new ArgumentException("TimeoutGracePeriod must not be negative");
        if (MaxAbandonedJobs < 0) throw new ArgumentException("MaxAbandonedJobs must not be negative");
        if (AbandonedJobAlertAfter < TimeSpan.Zero) throw new ArgumentException("AbandonedJobAlertAfter must not be negative");
        if (ShutdownTimeout < TimeSpan.Zero && ShutdownTimeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentException("ShutdownTimeout must not be negative, or Timeout.InfiniteTimeSpan");
        if (SchedulerMaxWait <= TimeSpan.Zero) throw new ArgumentException("SchedulerMaxWait must be positive");
    }

    internal static bool IsValidTimeout(TimeSpan t) => t > TimeSpan.Zero || t == Timeout.InfiniteTimeSpan;
}
