using BackgroundJobs.Demo.Events;

namespace BackgroundJobs.Demo.Jobs;

public sealed record SucceedAfterArgs(int DelayMs);
public sealed record FailNTimesArgs(int FailTimes);
public sealed record LogMessageArgs(string Message);

public sealed class SucceedAfterJob(EventLog events) : IJob<SucceedAfterArgs>
{
    public const string Name = "succeed-after";

    public async Task ExecuteAsync(SucceedAfterArgs args, JobContext context)
    {
        events.Started(context);
        await Task.Delay(args.DelayMs);
        events.Finished(context, ok: true);
    }
}

public sealed class FailNTimesJob(EventLog events) : IJob<FailNTimesArgs>
{
    public const string Name = "fail-n-times";

    public Task ExecuteAsync(FailNTimesArgs args, JobContext context)
    {
        events.Started(context);
        // The attempt number comes from the library, not from demo state.
        var fail = context.Attempt <= args.FailTimes;
        events.Finished(context, ok: !fail);
        return fail ? throw new InvalidOperationException("demo failure") : Task.CompletedTask;
    }
}

public sealed class LogMessageJob(EventLog events, ILogger<LogMessageJob> logger) : IJob<LogMessageArgs>
{
    public const string Name = "log-message";

    public Task ExecuteAsync(LogMessageArgs args, JobContext context)
    {
        events.Started(context);
        logger.LogInformation("log-message {JobId}: {Message}", context.JobId, args.Message);
        events.Message(context, args.Message);
        events.Finished(context, ok: true);
        return Task.CompletedTask;
    }
}
