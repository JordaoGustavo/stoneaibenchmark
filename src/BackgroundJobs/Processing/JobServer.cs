using BackgroundJobs.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// <summary>
/// A recurring job declared with BackgroundJobsBuilder.AddRecurringJob, upserted at every start.
/// </summary>
internal sealed record RecurringSeed(string Id, Func<IRecurringJobClient, CancellationToken, Task> Apply);

/// <summary>
/// Lifecycle: migrate → recover → start loops (ready). Stopping halts claims and promotions and waits
/// for running jobs, renewing their leases until they finish. It drains in StoppingAsync, before the web server stops, so HTTP keeps accepting enqueues.
/// </summary>
internal sealed class JobServer(
    IJobStore store,
    Dispatcher dispatcher,
    Scheduler scheduler,
    LeaseHeartbeat heartbeat,
    QueueMonitor queueMonitor,
    RetentionSweeper retention,
    IJobStoreListener listener,
    IRecurringJobClient recurring,
    IEnumerable<RecurringSeed> recurringSeeds,
    StoreCircuitBreaker storeBreaker,
    IOptions<BackgroundJobsOptions> options,
    TimeProvider time,
    ILogger<JobServer> logger) : IHostedLifecycleService
{
    private readonly CancellationTokenSource _stopping = new();
    private Task _run = Task.CompletedTask;
    private volatile bool _ready;

    public bool IsReady => _ready;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Start in the background so /health answers 503 while migrating and recovering.
        _run = Task.Run(() => RunAsync(_stopping.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        try
        {
            while (await storeBreaker.TryAsync("startup", async () =>
                   {
                       await store.MigrateAsync(ct);
                       // Only expired leases: a job still leased may belong to a live node, whatever its id.
                       var recovered = await store.RecoverAsync(time.UtcNow(), ct);
                       logger.LogInformation("recovered {Count} interrupted jobs", recovered);
                       await LeaseHeartbeat.RegisterAsync(store, o, time, ct);
                       foreach (var seed in recurringSeeds)
                       {
                           // A bad declaration is a code bug: report it, but do not keep the server from starting.
                           try { await seed.Apply(recurring, ct); }
                           catch (ArgumentException e) { logger.LogError(e, "recurring job {Id} was not declared", seed.Id); }
                       }
                       return true;
                   }, ct) != true)
                ct.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) { return; }

        // The heartbeat outlives the stop signal: running jobs are drained after it, and a job whose lease lapsed
        // meanwhile would be recovered and run a second time by another node. It stops once the dispatcher is done.
        using var heartbeatStop = new CancellationTokenSource();
        var draining = dispatcher.RunAsync(ct);
        var loops = new[]
        {
            DrainAsync(), scheduler.RunAsync(ct), heartbeat.RunAsync(heartbeatStop.Token), queueMonitor.RunAsync(ct), retention.RunAsync(ct),
            listener.RunAsync(ct),
        };
        _ready = true;
        logger.LogInformation("job server {NodeId} started; workers per queue: {Queues}", o.NodeId,
            string.Join(", ", o.ConsumedQueues.Select(q => $"{q.Key}={q.Value}")));
        await Task.WhenAll(loops);

        async Task DrainAsync()
        {
            try { await draining; }
            finally { await heartbeatStop.CancelAsync(); }
        }
    }

    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        _ready = false;
        if (_stopping.IsCancellationRequested) return;
        await _stopping.CancelAsync();
        // Deliberately not bounded by the host's token: running jobs are awaited.
        await _run;
        // Node ids are per instance, so no restart would ever replace this registration: drop it now rather than
        // leave the queues looking consumed until it lapses. After a crash it still lapses within a lease.
        try
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await store.RegisterConsumerAsync(options.Value.NodeId, [], time.UtcNow(), time.UtcNow(), bound.Token);
        }
        catch (Exception e) { logger.LogWarning(e, "could not deregister as queue consumer"); }
        logger.LogInformation("job server stopped");
    }

    /// <summary>
    /// The host calls StoppingAsync first; this covers hosts that only call StopAsync.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await StoppingAsync(cancellationToken);
        await _run;
        _stopping.Dispose();
    }

    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
