// Minimal use of the library without HTTP: register a job, enqueue it, let the hosted server run it.
using BackgroundJobs;

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(registerJobs: jobs => jobs.Register<GreetJob, GreetArgs>("greet"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IJobClient>();
var now = await client.EnqueueAsync("greet", new GreetArgs("now"));
var later = await client.EnqueueAsync("greet", new GreetArgs("in 2s"),
    new JobOptions { RunAt = DateTimeOffset.UtcNow.AddSeconds(2), MaxAttempts = 5, RetryDelay = TimeSpan.FromMilliseconds(500) });
Console.WriteLine($"enqueued {now} and scheduled {later}");

await host.WaitForShutdownAsync(); // Ctrl+C stops gracefully, waiting for running jobs.

// Args are persisted and read when the job runs, maybe after a deploy. Additive changes with defaults are safe;
// a breaking change bumps ArgsVersion and registers UpgradeArgs (docs/INTEGRACAO.md, "Versionando os args").
public sealed record GreetArgs(string Who);

public sealed class GreetJob(ILogger<GreetJob> logger) : IJob<GreetArgs>
{
    public Task ExecuteAsync(GreetArgs args, JobContext context)
    {
        logger.LogInformation("hello {Who} (job {JobId}, attempt {Attempt})", args.Who, context.JobId, context.Attempt);
        return Task.CompletedTask;
    }
}
