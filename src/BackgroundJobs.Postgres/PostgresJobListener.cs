using BackgroundJobs.Storage;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace BackgroundJobs.Postgres;

internal static class JobNotifications
{
    public const string Channel = "bj_jobs";
}

/// Owns the data source so the store and the listener share one pool.
internal sealed class JobDataSource(NpgsqlDataSource db) : IAsyncDisposable
{
    public NpgsqlDataSource Db => db;
    public ValueTask DisposeAsync() => db.DisposeAsync();
}

/// LISTENs on a dedicated connection and wakes the local loops, so other nodes' writes are seen in ms.
/// Polling remains the fallback while disconnected.
internal sealed class PostgresJobListener(JobDataSource source, IJobSignals signals, TimeProvider time, ILogger<PostgresJobListener> logger)
    : IJobStoreListener
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromMilliseconds(500), MaxBackoff = TimeSpan.FromSeconds(30);

    /// A connection whose peer vanished (no FIN, no RST) waits forever for notifications: after this long without one,
    /// a ping that must answer within PingTimeout proves the connection alive, or drops it to reconnect.
    private static readonly TimeSpan IdlePing = TimeSpan.FromSeconds(30);
    private const int PingTimeoutSeconds = 10;

    public async Task RunAsync(CancellationToken ct)
    {
        var backoff = MinBackoff;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await source.Db.OpenConnectionAsync(ct);
                conn.Notification += (_, e) => signals.Entered(JobStateWire.ParseState(e.Payload));
                await using (var cmd = new NpgsqlCommand($"LISTEN {JobNotifications.Channel}", conn))
                    await cmd.ExecuteNonQueryAsync(ct);
                backoff = MinBackoff;
                // Catch up on anything written while we were not listening.
                signals.JobsReady();
                signals.SchedulesChanged();
                while (true)
                {
                    if (await conn.WaitAsync(IdlePing, ct)) continue;
                    await using var ping = new NpgsqlCommand("SELECT 1", conn) { CommandTimeout = PingTimeoutSeconds };
                    await ping.ExecuteNonQueryAsync(ct);
                }
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
