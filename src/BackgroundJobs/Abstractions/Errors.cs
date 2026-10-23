namespace BackgroundJobs;

public sealed class JobNotFoundException(Guid id) : Exception($"job {id} not found")
{
    public Guid JobId { get; } = id;
}

/// The job is not in a state that allows the operation, or it changed concurrently.
public sealed class InvalidJobTransitionException(Guid id, JobState current, string operation)
    : Exception($"cannot {operation} job {id} in state {current.ToWire()}")
{
    public Guid JobId { get; } = id;
    public JobState CurrentState { get; } = current;
}

/// Requeueing the job would give it an idempotency key another unfinished job holds.
public sealed class IdempotencyKeyConflictException(Guid id, string key, Guid holderId)
    : Exception($"cannot requeue job {id}: idempotency key '{key}' is held by job {holderId}")
{
    public Guid JobId { get; } = id;
    public string IdempotencyKey { get; } = key;
    public Guid HolderJobId { get; } = holderId;
}

public sealed class RecurringJobNotFoundException(string id) : Exception($"recurring job {id} not found")
{
    public string RecurringJobId { get; } = id;
}
