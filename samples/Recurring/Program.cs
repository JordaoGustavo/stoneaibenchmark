// Recurring jobs: a cron definition declared in code, upserted at every start, each occurrence an ordinary job.
using BackgroundJobs;

// Its own queue, so other apps sharing the database (which would fail "tick" as an unknown job) never claim it.
const string Queue = "sample-recurring";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; }, registerJobs: jobs => jobs.Register<TickJob, TickArgs>("tick"))
    // Six fields means seconds first: every 2 seconds. Five fields is classic cron ("0 3 * * *" = daily at 03:00).
    .AddRecurringJob("sample-tick", "tick", "*/2 * * * * *", new TickArgs("every 2s"), new RecurringJobOptions { Queue = Queue })
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();

var recurring = host.Services.GetRequiredService<IRecurringJobClient>();
// Definitions can also be managed at runtime; this one reads its cron in a time zone.
await recurring.UpsertAsync("sample-weekday", "tick", "0 9 * * MON-FRI", new TickArgs("weekdays 09:00"),
    new RecurringJobOptions { Queue = Queue, TimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo") });
foreach (var r in await recurring.ListAsync())
    if (r.Id.StartsWith("sample-")) Console.WriteLine($"{r.Id}: '{r.Cron}' ({r.TimeZone}) next at {r.NextRunAt:O}");

await recurring.TriggerAsync("sample-weekday"); // one run now, outside the schedule
await Task.Delay(TimeSpan.FromSeconds(7));      // watch a few ticks

// Removing code declarations does not delete them from the store; remove explicitly.
await recurring.RemoveAsync("sample-tick");
await recurring.RemoveAsync("sample-weekday");
await host.StopAsync();

public sealed record TickArgs(string Label);

public sealed class TickJob(ILogger<TickJob> logger) : IJob<TickArgs>
{
    public Task ExecuteAsync(TickArgs args, JobContext context)
    {
        logger.LogInformation("tick {Label} at {Now:HH:mm:ss} (job {JobId})", args.Label, DateTime.UtcNow, context.JobId);
        return Task.CompletedTask;
    }
}
