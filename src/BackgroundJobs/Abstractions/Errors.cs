namespace BackgroundJobs;

/// <summary>
/// No job has this id: it never existed, or retention purged it after it finished.
/// </summary>
/// <param name="id">The id looked up.</param>
public sealed class JobNotFoundException(Guid id)
    : Exception($"job {id} not found: no job has this id, or it finished and was purged (see BackgroundJobsOptions.FinishedJobRetention)")
{
    /// <summary>The id looked up.</summary>
    public Guid JobId { get; } = id;
}

/// <summary>
/// The job is not in a state that allows the operation, or it changed concurrently.
/// </summary>
/// <param name="id">The job.</param>
/// <param name="current">Its state when the operation was refused.</param>
/// <param name="operation">The refused operation, e.g. "requeue" (<see cref="JobTransition.Operation"/>).</param>
public sealed class InvalidJobTransitionException(Guid id, JobState current, string operation)
    : Exception(Describe(id, current, operation))
{
    /// <summary>The job.</summary>
    public Guid JobId { get; } = id;

    /// <summary>Its state when the operation was refused.</summary>
    public JobState CurrentState { get; } = current;

    private static string Describe(Guid id, JobState current, string operation)
    {
        var message = $"cannot {operation} job {id}: it is {current.ToWire()}";
        if (JobTransition.FromOf(operation) is { } from)
            message += from.Contains(current)
                ? "; it changed while the operation ran, read it again and retry"
                : $"; {operation} applies only to {string.Join(", ", from.Select(s => s.ToWire()))} jobs";
        return message;
    }
}

/// <summary>
/// Requeueing the job would give it an idempotency key another unfinished job holds.
/// </summary>
/// <param name="id">The job being requeued.</param>
/// <param name="key">Its idempotency key.</param>
/// <param name="holderId">The unfinished job that holds the key.</param>
public sealed class IdempotencyKeyConflictException(Guid id, string key, Guid holderId)
    : Exception($"cannot requeue job {id}: idempotency key '{key}' is held by unfinished job {holderId}; " +
                $"wait until job {holderId} finishes, or cancel or delete it, then requeue")
{
    /// <summary>The job being requeued.</summary>
    public Guid JobId { get; } = id;

    /// <summary>Its idempotency key.</summary>
    public string IdempotencyKey { get; } = key;

    /// <summary>The unfinished job that holds the key.</summary>
    public Guid HolderJobId { get; } = holderId;
}

/// <summary>
/// No recurring definition has this id.
/// </summary>
/// <param name="id">The id looked up.</param>
public sealed class RecurringJobNotFoundException(string id)
    : Exception($"recurring job '{id}' not found; create it with IRecurringJobClient.UpsertAsync or BackgroundJobsBuilder.AddRecurringJob")
{
    /// <summary>The id looked up.</summary>
    public string RecurringJobId { get; } = id;
}
