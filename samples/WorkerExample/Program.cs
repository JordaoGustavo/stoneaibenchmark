// Minimal use of the library without HTTP: register a job, enqueue it, let the hosted server run it.
using BackgroundJobs;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddBackgroundJobs(
    o => o.ConnectionString = Environment.GetEnvironmentVariable("DATABASE_URL")
                              ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs",
    jobs => jobs.Register<GreetJob, GreetArgs>("greet"));

var host = builder.Build();
await host.StartAsync();

var client = host.Services.GetRequiredService<IJobClient>();
var now = await client.EnqueueAsync("greet", new GreetArgs("now"));
var later = await client.EnqueueAsync("greet", new GreetArgs("in 2s"),
    new JobOptions { RunAt = DateTimeOffset.UtcNow.AddSeconds(2), MaxAttempts = 5, RetryDelay = TimeSpan.FromMilliseconds(500) });
Console.WriteLine($"enqueued {now} and scheduled {later}");

await host.WaitForShutdownAsync(); // Ctrl+C stops gracefully, waiting for running jobs.

public sealed record GreetArgs(string Who);

public sealed class GreetJob(ILogger<GreetJob> logger) : IJob<GreetArgs>
{
    public Task ExecuteAsync(GreetArgs args, JobContext context)
    {
        logger.LogInformation("hello {Who} (job {JobId}, attempt {Attempt})", args.Who, context.JobId, context.Attempt);
        return Task.CompletedTask;
    }
}
