// Per-job MaxConcurrency: at most N jobs of a name process at once across the whole cluster, whatever the worker count.
using BackgroundJobs;

// Its own queue, so other apps sharing the database (which would fail "report" as an unknown job) never claim it.
const string Queue = "sample-concurrency";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o =>
        {
            o.FinishedJobRetention = TimeSpan.FromDays(7);
            o.Queues = new() { [Queue] = 8 }; // plenty of workers: the cap, not the pool, limits the job below
        },
        registerJobs: jobs => jobs.Register<ReportJob, ReportArgs>("report", o => o.MaxConcurrency = 2))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

var ids = new List<Guid>();
for (var i = 0; i < 6; i++) ids.Add(await client.EnqueueAsync("report", new ReportArgs(i), new JobOptions { Queue = Queue }));
foreach (var id in ids)
    while ((await client.GetAsync(id)).State is not (JobState.Succeeded or JobState.Failed)) await Task.Delay(100);
Console.WriteLine($"6 jobs done; most running at once: {ReportJob.MaxSeen} (cap 2)");

await host.StopAsync();

public sealed record ReportArgs(int Number);

public sealed class ReportJob(ILogger<ReportJob> logger) : IJob<ReportArgs>
{
    private static int _running;
    public static int MaxSeen;

    public async Task ExecuteAsync(ReportArgs args, JobContext context)
    {
        var now = Interlocked.Increment(ref _running);
        InterlockedMax(ref MaxSeen, now);
        logger.LogInformation("report {Number} started, {Running} running", args.Number, now);
        try { await Task.Delay(1000, context.CancellationToken); }
        finally { Interlocked.Decrement(ref _running); }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while (value > (seen = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen) { }
    }
}
