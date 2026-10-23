// Continuations: jobs that wait in `awaiting` until a parent job finishes, then run or are cancelled by Condition.
using BackgroundJobs;

const string Queue = "sample-continuations";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; },
        registerJobs: jobs => jobs.Register<StepJob, StepArgs>("step"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

// A chain: export -> compress -> upload. Each child is created right away and stays `awaiting` until its parent ends.
var export = await client.EnqueueAsync("step", new StepArgs("export", Fail: false), new JobOptions { Queue = Queue });
var compress = await client.EnqueueAsync("step", new StepArgs("compress", Fail: false),
    new JobOptions { Queue = Queue, ContinueAfter = export });                       // Condition defaults to OnSucceeded
var upload = await client.EnqueueAsync("step", new StepArgs("upload", Fail: false),
    new JobOptions { Queue = Queue, ContinueAfter = compress });
Console.WriteLine($"upload right after enqueue: {(await client.GetAsync(upload)).State}");

// A failing parent: the OnSucceeded child is cancelled, the OnFinished child (e.g. cleanup/notify) runs anyway.
var risky = await client.EnqueueAsync("step", new StepArgs("risky", Fail: true), new JobOptions { Queue = Queue, MaxAttempts = 1 });
var onSuccess = await client.EnqueueAsync("step", new StepArgs("publish", Fail: false),
    new JobOptions { Queue = Queue, ContinueAfter = risky, Condition = ContinuationCondition.OnSucceeded });
var always = await client.EnqueueAsync("step", new StepArgs("cleanup", Fail: false),
    new JobOptions { Queue = Queue, ContinueAfter = risky, Condition = ContinuationCondition.OnFinished });

var ids = new[] { ("export", export), ("compress", compress), ("upload", upload), ("risky", risky), ("publish", onSuccess), ("cleanup", always) };
while (true)
{
    var jobs = await Task.WhenAll(ids.Select(async x => (x.Item1, Job: await client.GetAsync(x.Item2))));
    if (jobs.All(j => j.Job.State is JobState.Succeeded or JobState.Failed or JobState.Cancelled))
    {
        foreach (var (name, job) in jobs)
            Console.WriteLine($"{name,-9} {job.State,-10} {job.History[^1].Reason}");
        break;
    }
    await Task.Delay(100);
}

await host.StopAsync();

public sealed record StepArgs(string Step, bool Fail);

public sealed class StepJob(ILogger<StepJob> logger) : IJob<StepArgs>
{
    public Task ExecuteAsync(StepArgs args, JobContext context)
    {
        logger.LogInformation("running {Step}", args.Step);
        return args.Fail ? throw new InvalidOperationException($"{args.Step} failed") : Task.CompletedTask;
    }
}
