// Idempotency keys: enqueueing the same key while the first job is unfinished returns the first job's id.
using BackgroundJobs;

// Its own queue, so other apps sharing the database (which would fail "charge" as an unknown job) never claim it.
const string Queue = "sample-idempotency";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; }, registerJobs: jobs => jobs.Register<ChargeJob, ChargeArgs>("charge"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

// Typically a key derived from the business operation, e.g. a request or order id. Scheduled in 2s so it stays unfinished.
var key = $"charge-order-{Guid.NewGuid():N}";
var options = new JobOptions { Queue = Queue, IdempotencyKey = key, RunAt = DateTimeOffset.UtcNow.AddSeconds(2) };
var first = await client.EnqueueAsync("charge", new ChargeArgs(100), options);
var second = await client.EnqueueAsync("charge", new ChargeArgs(100), options); // e.g. a retried HTTP request
Console.WriteLine($"first  {first}\nsecond {second}\nsame id: {first == second}");

// Once the job finishes the key is free again, so a later enqueue creates a new job.
while ((await client.GetAsync(first)).State != JobState.Succeeded) await Task.Delay(200);
var third = await client.EnqueueAsync("charge", new ChargeArgs(100), new JobOptions { Queue = Queue, IdempotencyKey = key });
Console.WriteLine($"after it succeeded: {third} (new job: {third != first})");

await Task.Delay(500);
await host.StopAsync();

public sealed record ChargeArgs(int AmountCents);

public sealed class ChargeJob(ILogger<ChargeJob> logger) : IJob<ChargeArgs>
{
    public Task ExecuteAsync(ChargeArgs args, JobContext context)
    {
        logger.LogInformation("charged {Amount} cents (job {JobId})", args.AmountCents, context.JobId);
        return Task.CompletedTask;
    }
}
