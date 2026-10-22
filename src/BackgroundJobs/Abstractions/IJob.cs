namespace BackgroundJobs;

/// A unit of work registered under a stable name. Throwing fails the attempt.
public interface IJob<in TArgs>
{
    Task ExecuteAsync(TArgs args, JobContext context);
}

public sealed record JobContext(Guid JobId, int Attempt);
