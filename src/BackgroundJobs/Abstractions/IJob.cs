namespace BackgroundJobs;

/// A unit of work registered under a stable name. Throwing fails the attempt.
public interface IJob<in TArgs>
{
    Task ExecuteAsync(TArgs args, JobContext context);
}

/// <param name="CancellationToken">Fires when the attempt times out or the server stops. Pass it on to the calls the job
/// makes; a job that ignores it keeps its worker until it returns (on timeout, until the grace period ends).</param>
public sealed record JobContext(Guid JobId, int Attempt, CancellationToken CancellationToken = default);
