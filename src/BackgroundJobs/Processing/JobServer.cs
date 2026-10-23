using BackgroundJobs.Storage;
using BackgroundJobs.Storage.Postgres;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Lifecycle: migrate → recover → start loops (ready). Stopping halts claims and promotions and waits
/// for running jobs. It drains in StoppingAsync, before the web server stops, so HTTP keeps accepting enqueues.
internal sealed class JobServer(
    IJobStore store,
    Dispatcher dispatcher,
    Scheduler scheduler,
    LeaseHeartbeat heartbeat,
    PostgresJobListener listener,
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
                       // Our own node id: jobs it held were interrupted by our previous crash; no need to wait for the lease.
                       var recovered = await store.RecoverAsync(time.UtcNow(), o.NodeId, ct);
                       logger.LogInformation("recovered {Count} interrupted jobs", recovered);
                       return true;
                   }, ct) != true)
                ct.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) { return; }

        var loops = new[] { dispatcher.RunAsync(ct), scheduler.RunAsync(ct), heartbeat.RunAsync(ct), listener.RunAsync(ct) };
        _ready = true;
        logger.LogInformation("job server {NodeId} started with {Workers} workers", o.NodeId, o.WorkerCount);
        await Task.WhenAll(loops);
    }

    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        _ready = false;
        if (_stopping.IsCancellationRequested) return;
        await _stopping.CancelAsync();
        // Deliberately not bounded by the host's token: running jobs are awaited.
        await _run;
        logger.LogInformation("job server stopped");
    }

    /// The host calls StoppingAsync first; this covers hosts that only call StopAsync.
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
