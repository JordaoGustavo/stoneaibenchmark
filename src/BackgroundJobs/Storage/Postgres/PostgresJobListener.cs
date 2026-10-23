using BackgroundJobs.Processing;
using BackgroundJobs.Storage;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace BackgroundJobs.Storage.Postgres;

internal static class JobNotifications
{
    public const string Channel = "bj_jobs";
    public const string Enqueued = "enqueued";
    public const string Scheduled = "scheduled";
}

/// Owns the data source so the store and the listener share one pool.
internal sealed class JobDataSource(NpgsqlDataSource db) : IAsyncDisposable
{
    public NpgsqlDataSource Db => db;
    public ValueTask DisposeAsync() => db.DisposeAsync();
}

/// LISTENs on a dedicated connection and wakes the local loops, so other nodes' writes are seen in ms.
/// Polling remains the fallback while disconnected.
internal sealed class PostgresJobListener(JobDataSource source, Signals signals, TimeProvider time, ILogger<PostgresJobListener> logger)
    : IJobStoreListener
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromMilliseconds(500), MaxBackoff = TimeSpan.FromSeconds(30);

    public async Task RunAsync(CancellationToken ct)
    {
        var backoff = MinBackoff;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await source.Db.OpenConnectionAsync(ct);
                conn.Notification += (_, e) => (e.Payload == JobNotifications.Scheduled ? signals.Scheduler : signals.Dispatcher).Set();
                await using (var cmd = new NpgsqlCommand($"LISTEN {JobNotifications.Channel}", conn))
                    await cmd.ExecuteNonQueryAsync(ct);
                backoff = MinBackoff;
                // Catch up on anything written while we were not listening.
                signals.Dispatcher.Set();
                signals.Scheduler.Set();
                while (true) await conn.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch (Exception e)
            {
                logger.LogWarning(e, "job listener disconnected; retrying in {Backoff}", backoff);
                try { await Task.Delay(backoff, time, ct); }
                catch (OperationCanceledException) { return; }
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, MaxBackoff.Ticks));
            }
        }
    }
}
