namespace BackgroundJobs;

/// <summary>
/// A unit of work registered under a stable name. Throwing fails the attempt.
/// </summary>
public interface IJob<in TArgs>
{
    /// <summary>
    /// Runs one attempt. Called on a new DI scope per attempt; <typeparamref name="TArgs"/> is deserialized from the
    /// stored JSON with <see cref="JobJson.Options"/>.
    /// </summary>
    Task ExecuteAsync(TArgs args, JobContext context);
}

/// <summary>
/// What a running attempt knows about itself.
/// </summary>
/// <param name="JobId">The job's id; the same across attempts.</param>
/// <param name="Attempt">1-based attempt number; keeps growing across refunded attempts.</param>
/// <param name="CancellationToken">Fires when the attempt times out or the server stops. Pass it on to the calls the job
/// makes; a job that ignores it keeps its worker until it returns (on timeout, until the grace period ends).</param>
public sealed record JobContext(Guid JobId, int Attempt, CancellationToken CancellationToken = default);
