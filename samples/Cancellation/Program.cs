// Cancelling a running job: CancelAsync fires the job's CancellationToken and the job ends cancelled.
using BackgroundJobs;

// Its own queue, so other apps sharing the database (which would fail "export" as an unknown job) never claim it.
const string Queue = "sample-cancellation";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; }, registerJobs: jobs => jobs.Register<ExportJob, ExportArgs>("export"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

var id = await client.EnqueueAsync("export", new ExportArgs(Rows: 1_000), new JobOptions { Queue = Queue });
while ((await client.GetAsync(id)).State != JobState.Processing) await Task.Delay(100);
await Task.Delay(1500); // let it make some progress

await client.CancelAsync(id); // a scheduled or enqueued job is cancelled at once; a processing one via its token
JobState state;
while ((state = (await client.GetAsync(id)).State) == JobState.Processing) await Task.Delay(100);
Console.WriteLine($"job {id} ended {state.ToWire()}");

await host.StopAsync();

public sealed record ExportArgs(int Rows);

public sealed class ExportJob(ILogger<ExportJob> logger) : IJob<ExportArgs>
{
    public async Task ExecuteAsync(ExportArgs args, JobContext context)
    {
        for (var row = 0; row < args.Rows; row++)
        {
            // Pass the token to every call (it also fires on timeout and shutdown), or check it between steps.
            await Task.Delay(100, context.CancellationToken);
            if (row % 5 == 0) logger.LogInformation("exported row {Row}", row);
        }
    }
}
