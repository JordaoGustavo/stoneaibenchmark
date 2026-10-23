// Exponential retry backoff: a job that fails three times, waiting ~0.5s, 1s, 2s between attempts (capped at 5s).
using BackgroundJobs;

const string Queue = "sample-backoff";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o =>
    {
        o.FinishedJobRetention = TimeSpan.FromDays(7);
        o.Queues = new() { [Queue] = 2 };
        // Node-wide defaults (also used by recurring jobs); the JobOptions below override them per job.
        o.DefaultRetryBackoff = RetryBackoff.Exponential;
        o.DefaultRetryBackoffMultiplier = 2;
        o.DefaultMaxRetryDelay = TimeSpan.FromMinutes(5);
    }, registerJobs: jobs => jobs.Register<FlakyJob, FlakyArgs>("flaky"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

// The retry after attempt n waits RetryDelay * RetryBackoffMultiplier^(n-1), capped at MaxRetryDelay, + jitter.
var id = await client.EnqueueAsync("flaky", new FlakyArgs(FailTimes: 3), new JobOptions
{
    Queue = Queue,
    MaxAttempts = 5,
    RetryDelay = TimeSpan.FromMilliseconds(500),
    RetryBackoff = RetryBackoff.Exponential,
    RetryBackoffMultiplier = 2,
    MaxRetryDelay = TimeSpan.FromSeconds(5),
});

Job job;
while ((job = await client.GetAsync(id)).State is not (JobState.Succeeded or JobState.Failed)) await Task.Delay(100);
Console.WriteLine($"job {id} ended {job.State} after {job.Attempts} attempts");
DateTime? previousEnd = null;
foreach (var a in job.AttemptHistory)
{
    var gap = previousEnd is { } end ? $" (started {(a.StartedAt - end).TotalSeconds:0.0}s after the previous one)" : "";
    Console.WriteLine($"  attempt {a.Attempt}: {a.Outcome}{gap}");
    previousEnd = a.FinishedAt;
}

await host.StopAsync();

public sealed record FlakyArgs(int FailTimes);

public sealed class FlakyJob : IJob<FlakyArgs>
{
    public Task ExecuteAsync(FlakyArgs args, JobContext context) =>
        context.Attempt <= args.FailTimes
            ? throw new InvalidOperationException($"transient failure on attempt {context.Attempt}")
            : Task.CompletedTask;
}
