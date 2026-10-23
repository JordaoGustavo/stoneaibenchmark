namespace BackgroundJobs;

/// <summary>
/// Per-job options. Unset fields fall back to the defaults in <see cref="BackgroundJobsOptions"/>.
/// </summary>
public sealed record JobOptions
{
    /// <summary>
    /// Not before this instant. Null or in the past means "enqueue now".
    /// </summary>
    public DateTimeOffset? RunAt { get; init; }

    /// <summary>
    /// Total executions allowed, including the first one.
    /// </summary>
    public int? MaxAttempts { get; init; }

    /// <summary>
    /// Delay before each retry; with <see cref="RetryBackoff"/> Exponential, the delay before the first retry.
    /// </summary>
    public TimeSpan? RetryDelay { get; init; }

    /// <summary>
    /// Random extra delay in [0, RetryJitter] added to each retry.
    /// </summary>
    public TimeSpan? RetryJitter { get; init; }

    /// <summary>
    /// How the delay grows between retries. Defaults to <see cref="BackgroundJobsOptions.DefaultRetryBackoff"/>
    /// (<see cref="BackgroundJobs.RetryBackoff.Fixed"/> unless configured).
    /// </summary>
    public RetryBackoff? RetryBackoff { get; init; }

    /// <summary>
    /// With <see cref="BackgroundJobs.RetryBackoff.Exponential"/>, each retry waits this many times the previous one: from 1 to 100.
    /// Ignored by <see cref="BackgroundJobs.RetryBackoff.Fixed"/>. Defaults to <see cref="BackgroundJobsOptions.DefaultRetryBackoffMultiplier"/>.
    /// </summary>
    public double? RetryBackoffMultiplier { get; init; }

    /// <summary>
    /// With <see cref="BackgroundJobs.RetryBackoff.Exponential"/>, the longest delay before a retry (jitter is added on top);
    /// <see cref="Timeout.InfiniteTimeSpan"/> for no cap. Ignored by <see cref="BackgroundJobs.RetryBackoff.Fixed"/>.
    /// Defaults to <see cref="BackgroundJobsOptions.DefaultMaxRetryDelay"/>.
    /// </summary>
    public TimeSpan? MaxRetryDelay { get; init; }

    /// <summary>
    /// The queue the job waits in; only servers consuming it (<see cref="BackgroundJobsOptions.Queues"/>) run it.
    /// Defaults to <see cref="JobQueues.Default"/>. See <see cref="JobQueues.IsValidName"/>.
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Higher runs first within its queue, between <see cref="JobQueues.MinPriority"/> and <see cref="JobQueues.MaxPriority"/>.
    /// Defaults to <see cref="JobQueues.DefaultPriority"/>.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// While a job enqueued with this key is not finished (scheduled, enqueued or processing), enqueueing the key again
    /// creates nothing and returns that job's id, whatever the new name, args and options. Once it finished, the key is free. Up to <see cref="MaxIdempotencyKeyLength"/>
    /// characters, not blank. Null: no deduplication.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>
    /// Longest accepted <see cref="IdempotencyKey"/>, in characters.
    /// </summary>
    public const int MaxIdempotencyKeyLength = 200;

    internal static void CheckIdempotencyKey(string? key)
    {
        if (key == null) return;
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("JobOptions.IdempotencyKey is blank; pass a non-blank key, or null to enqueue without deduplication");
        if (key.Length > MaxIdempotencyKeyLength)
            throw new ArgumentException(
                $"JobOptions.IdempotencyKey is {key.Length} characters long; use at most {MaxIdempotencyKeyLength} (hash a longer natural key)");
    }
}

/// <summary>
/// Server-wide settings, configured with <c>AddBackgroundJobs(o => ...)</c> and validated when the server starts.
/// </summary>
public sealed class BackgroundJobsOptions
{
    /// <summary>
    /// Workers for <see cref="JobQueues.Default"/> when <see cref="Queues"/> is empty; ignored otherwise.
    /// </summary>
    public int WorkerCount { get; set; } = 4;

    /// <summary>
    /// The queues this server consumes and how many workers each one gets; each queue has its own workers, so a busy
    /// queue never holds up another. Empty: only <see cref="JobQueues.Default"/>, with <see cref="WorkerCount"/> workers.
    /// Jobs in a queue no server consumes stay enqueued.
    /// </summary>
    public Dictionary<string, int> Queues { get; set; } = [];

    /// <summary>
    /// <see cref="Queues"/>, or the default queue with <see cref="WorkerCount"/> workers when it is empty.
    /// </summary>
    internal IReadOnlyDictionary<string, int> ConsumedQueues =>
        Queues.Count > 0 ? Queues : new Dictionary<string, int> { [JobQueues.Default] = WorkerCount };

    internal int TotalWorkers => ConsumedQueues.Values.Sum();

    /// <summary>
    /// Identifies this process as lease owner; must be unique among live processes. The default (machine name plus a
    /// random suffix) is unique per instance, so several processes on one machine or image never share it.
    /// </summary>
    public string NodeId { get; set; } = UniqueNodeId();

    private static string UniqueNodeId() => $"{Environment.MachineName}-{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>
    /// How long a claimed job stays locked to this node without a renewal; renewed every LeaseDuration / 3. A job whose
    /// node died is recovered once its lease expires.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often idle workers poll the store when the store has no change notifications, or as a fallback to them.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Longest the scheduler sleeps without checking the store; a fallback for missed notifications.
    /// </summary>
    public TimeSpan SchedulerMaxWait { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Every this many jobs claimed from a queue, one is its oldest ready job whatever its priority, so a steady stream of
    /// high-priority jobs cannot starve the low ones: they get at least 1/FifoClaimEvery of the claims. Null: strict
    /// priority. Must be >= 2.
    /// </summary>
    public int? FifoClaimEvery { get; set; } = 10;

    /// <summary>
    /// Max jobs claimed per query, per queue. Null: as many as the queue has free worker slots.
    /// </summary>
    public int? ClaimBatchSize { get; set; }

    /// <summary>
    /// <see cref="JobOptions.MaxAttempts"/> when a job does not set it.
    /// </summary>
    public int DefaultMaxAttempts { get; set; } = 3;

    /// <summary>
    /// <see cref="JobOptions.RetryDelay"/> when a job does not set it.
    /// </summary>
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// <see cref="JobOptions.RetryJitter"/> when a job does not set it.
    /// </summary>
    public TimeSpan DefaultRetryJitter { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// <see cref="JobOptions.RetryBackoff"/> when a job does not set it. Also used by jobs that recurring definitions enqueue.
    /// </summary>
    public RetryBackoff DefaultRetryBackoff { get; set; } = RetryBackoff.Fixed;

    /// <summary>
    /// <see cref="JobOptions.RetryBackoffMultiplier"/> when a job does not set it. From 1 to 100.
    /// </summary>
    public double DefaultRetryBackoffMultiplier { get; set; } = 2;

    /// <summary>
    /// <see cref="JobOptions.MaxRetryDelay"/> when a job does not set it. No cap (<see cref="Timeout.InfiniteTimeSpan"/>) by default.
    /// </summary>
    public TimeSpan DefaultMaxRetryDelay { get; set; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// Longest an attempt may run before its <see cref="JobContext.CancellationToken"/> fires and the attempt fails.
    /// <see cref="JobRegistrationOptions.Timeout"/> overrides it per job name. Infinite by default.
    /// </summary>
    public TimeSpan DefaultJobTimeout { get; set; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// After a timeout or a cancel, how long a job that ignores its token keeps its worker. Then the worker is freed for
    /// other jobs while the job's code keeps running; the job stays processing (its lease renewed, so it never runs twice)
    /// until the code returns, and that result is recorded.
    /// </summary>
    public TimeSpan TimeoutGracePeriod { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Most jobs that may run past their grace period at once without a worker. Beyond it, such a job keeps its worker.
    /// Bounds the threads a pile of stuck jobs can take: at most (all queues' workers) + MaxAbandonedJobs jobs run at once.
    /// </summary>
    public int MaxAbandonedJobs { get; set; } = 4;

    /// <summary>
    /// The health check reports degraded once a job has run without its worker for longer than this. A job whose code
    /// never returns stays processing for good, so this is how it gets noticed.
    /// </summary>
    public TimeSpan AbandonedJobAlertAfter { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// When the server stops, how long running jobs are waited for after their token fired. Jobs still running then are
    /// recorded as interrupted and requeued (their attempt given back), and the server stops. Infinite by default.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = Timeout.InfiniteTimeSpan;

    /// <summary>
    /// How often the ready queue is sampled for metrics and the health check.
    /// </summary>
    public TimeSpan QueueMonitorInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The health check reports a stalled queue when a job has been ready for longer than this.
    /// </summary>
    public TimeSpan StalledQueueThreshold { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Required: how long a finished job (succeeded, failed or deleted) is kept, with its history and attempts, after it
    /// finished. Older ones are purged. <see cref="Timeout.InfiniteTimeSpan"/> keeps them forever (the tables then grow
    /// without bound). There is no default: how much history to keep is a decision of the application.
    /// </summary>
    public TimeSpan? FinishedJobRetention { get; set; }

    /// <summary>
    /// How often finished jobs past their retention are purged.
    /// </summary>
    public TimeSpan RetentionSweepInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Max jobs purged per store call; a sweep repeats until fewer remain.
    /// </summary>
    public int RetentionBatchSize { get; set; } = 1000;

    internal void Validate()
    {
        if (FinishedJobRetention == null)
            throw new ArgumentException(
                "BackgroundJobsOptions.FinishedJobRetention is not set; there is no default. Configure how long finished jobs are kept in " +
                "AddBackgroundJobs(o => o.FinishedJobRetention = TimeSpan.FromDays(7)), or Timeout.InfiniteTimeSpan to keep them forever");
        if (FinishedJobRetention <= TimeSpan.Zero && FinishedJobRetention != Timeout.InfiniteTimeSpan)
            throw Invalid(nameof(FinishedJobRetention), FinishedJobRetention, "positive, or Timeout.InfiniteTimeSpan to keep finished jobs forever");
        if (RetentionSweepInterval <= TimeSpan.Zero) throw Invalid(nameof(RetentionSweepInterval), RetentionSweepInterval, "positive");
        if (RetentionBatchSize < 1) throw Invalid(nameof(RetentionBatchSize), RetentionBatchSize, "at least 1");
        if (WorkerCount < 1) throw Invalid(nameof(WorkerCount), WorkerCount, "at least 1");
        foreach (var (queue, workers) in Queues)
        {
            if (!JobQueues.IsValidName(queue))
                throw new ArgumentException(
                    $"BackgroundJobsOptions.Queues has an invalid queue name '{queue}'; {JobQueues.NameRule}. " +
                    "Fix the key in AddBackgroundJobs(o => o.Queues[\"name\"] = workers)");
            if (workers < 1)
                throw new ArgumentException(
                    $"BackgroundJobsOptions.Queues['{queue}'] is {workers}, but it must be at least 1 worker; " +
                    $"set it in AddBackgroundJobs(o => o.Queues[\"{queue}\"] = ...), or remove the queue so this node does not consume it");
        }
        if (ClaimBatchSize < 1) throw Invalid(nameof(ClaimBatchSize), ClaimBatchSize, "at least 1, or null to claim as many jobs as there are free workers");
        if (FifoClaimEvery < 2) throw Invalid(nameof(FifoClaimEvery), FifoClaimEvery, "at least 2, or null for strict priority");
        if (LeaseDuration <= TimeSpan.Zero) throw Invalid(nameof(LeaseDuration), LeaseDuration, "positive");
        if (QueueMonitorInterval <= TimeSpan.Zero) throw Invalid(nameof(QueueMonitorInterval), QueueMonitorInterval, "positive");
        if (StalledQueueThreshold <= TimeSpan.Zero) throw Invalid(nameof(StalledQueueThreshold), StalledQueueThreshold, "positive");
        if (!IsValidTimeout(DefaultJobTimeout))
            throw Invalid(nameof(DefaultJobTimeout), DefaultJobTimeout, "positive, or Timeout.InfiniteTimeSpan for no timeout");
        if (TimeoutGracePeriod < TimeSpan.Zero) throw Invalid(nameof(TimeoutGracePeriod), TimeoutGracePeriod, "zero or positive");
        if (MaxAbandonedJobs < 0) throw Invalid(nameof(MaxAbandonedJobs), MaxAbandonedJobs, "zero or positive");
        if (AbandonedJobAlertAfter < TimeSpan.Zero) throw Invalid(nameof(AbandonedJobAlertAfter), AbandonedJobAlertAfter, "zero or positive");
        if (ShutdownTimeout < TimeSpan.Zero && ShutdownTimeout != Timeout.InfiniteTimeSpan)
            throw Invalid(nameof(ShutdownTimeout), ShutdownTimeout, "zero or positive, or Timeout.InfiniteTimeSpan to wait for running jobs");
        if (!Enum.IsDefined(DefaultRetryBackoff)) throw Invalid(nameof(DefaultRetryBackoff), DefaultRetryBackoff, "Fixed or Exponential");
        if (!Processing.BackoffSettings.IsValidMultiplier(DefaultRetryBackoffMultiplier))
            throw Invalid(nameof(DefaultRetryBackoffMultiplier), DefaultRetryBackoffMultiplier, $"a finite number from 1 to {Processing.BackoffSettings.MaxMultiplier}");
        if (!Processing.BackoffSettings.IsValidMaxDelay(DefaultMaxRetryDelay))
            throw Invalid(nameof(DefaultMaxRetryDelay), DefaultMaxRetryDelay,
                $"from zero to {TimeSpan.FromMilliseconds(int.MaxValue)}, or Timeout.InfiniteTimeSpan for no cap");
        if (SchedulerMaxWait <= TimeSpan.Zero) throw Invalid(nameof(SchedulerMaxWait), SchedulerMaxWait, "positive");
    }

    private static ArgumentException Invalid(string option, object? value, string rule) =>
        new($"BackgroundJobsOptions.{option} is {value}, but it must be {rule}; set it in AddBackgroundJobs(o => o.{option} = ...)");

    internal static bool IsValidTimeout(TimeSpan t) => t > TimeSpan.Zero || t == Timeout.InfiniteTimeSpan;
}
