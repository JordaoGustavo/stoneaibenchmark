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

/// "greet" before its args changed (args version 1). Kept so tests can run the old code against the same database.
public sealed record GreetV1Args(string Who);

public sealed class GreetV1Job(EventLog events) : IJob<GreetV1Args>
{
    public Task ExecuteAsync(GreetV1Args args, JobContext context)
    {
        events.Started(context);
        events.Message(context, $"hello {args.Who}");
        events.Finished(context, ok: true);
        return Task.CompletedTask;
    }
}

/// "greet" today: the single name was split in two, a breaking change, so the args version went to 2.
public sealed record GreetArgs(string FirstName, string LastName);

public sealed class GreetJob(EventLog events) : IJob<GreetArgs>
{
    public const string Name = "greet";

    /// v1 {who: "Ada Lovelace"} → v2 {firstName: "Ada", lastName: "Lovelace"}.
    public static System.Text.Json.Nodes.JsonObject UpgradeFromV1(System.Text.Json.Nodes.JsonObject v1)
    {
        var parts = (v1["who"]?.GetValue<string>() ?? "").Split(' ', 2);
        return new() { ["firstName"] = parts[0], ["lastName"] = parts.Length > 1 ? parts[1] : "" };
    }

    public Task ExecuteAsync(GreetArgs args, JobContext context)
    {
        events.Started(context);
        events.Message(context, $"hello {args.FirstName} ({args.LastName})");
        events.Finished(context, ok: true);
        return Task.CompletedTask;
    }
}
