using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// A job runs on one node at a time: a lease is never let lapse while its job runs, a node that lost a lease stops the
/// job, and leases mean the same thing on every node whatever their clocks say.
[Collection(E2ECollection.Name)]
public sealed class S21_LeaseSafety(PostgresFixture pg)
{
    [Fact]
    public async Task A_job_drained_at_stop_keeps_its_lease_past_the_lease_duration()
    {
        var db = await pg.CreateDatabaseAsync();
        var stopping = await DemoProcess.StartAsync(db, workers: 1, env: DemoProcess.ShortLease);

        // Ignores its token, so the stop waits for it: three leases long.
        var id = await stopping.Api.EnqueueAsync("succeed-after", new { delayMs = 4500 }, new { maxAttempts = 3 });
        await stopping.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        await using var other = await DemoProcess.StartAsync(db, workers: 1, env: DemoProcess.ShortLease);
        stopping.Terminate();
        await stopping.WaitForExitAsync(TimeSpan.FromSeconds(15));

        var job = await other.Api.WaitForStateAsync(id, "succeeded");
        Assert.Equal("succeeded", Assert.Single(job.AttemptHistory).Outcome);
        Assert.DoesNotContain(await other.Api.EventsAsync(), e => e.JobId == id);
        await stopping.DisposeAsync();
    }

    [Fact]
    public async Task A_node_cut_off_from_the_database_longer_than_a_lease_stops_the_job_the_other_node_took_over()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var proxy = new TcpProxy(db);
        await using var cutOff = await DemoProcess.StartAsync(proxy.ConnectionString, workers: 1, env: DemoProcess.ShortLease);

        var id = await cutOff.Api.EnqueueAsync("sleep", new { delayMs = 12_000 }, new { maxAttempts = 3 });
        await cutOff.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        await using var survivor = await DemoProcess.StartAsync(db, workers: 1, env: DemoProcess.ShortLease);

        // The cut-off node cannot renew; once its last renewal is a lease old, it must assume the survivor took the job.
        proxy.Cut();
        var stopped = await cutOff.Api.WaitForEventsAsync(
            e => e.Any(x => x.JobId == id && x.Type == "message" && x.Detail?.GetProperty("message").GetString() == "cancelled"),
            TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(stopped, e => e.JobId == id && e.Type == "finished");
        await survivor.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"), TimeSpan.FromSeconds(10));
        proxy.Restore();

        var job = await survivor.Api.WaitForStateAsync(id, "succeeded", TimeSpan.FromSeconds(20));
        Assert.Equal(["interrupted", "succeeded"], job.AttemptHistory.Select(a => a.Outcome));
        Assert.DoesNotContain(await cutOff.Api.EventsAsync(), e => e.JobId == id && e.Type == "finished");
    }

    [Fact]
    public async Task A_node_that_finds_its_lease_taken_over_stops_the_job()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var node = await DemoProcess.StartAsync(db, workers: 1, env: DemoProcess.ShortLease);

        var id = await node.Api.EnqueueAsync("sleep", new { delayMs = 12_000 }, new { maxAttempts = 3 });
        await node.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        // As another node's claim would leave it after recovering the job.
        await using (var conn = new Npgsql.NpgsqlConnection(db))
        {
            await conn.OpenAsync();
            await using var cmd = new Npgsql.NpgsqlCommand("UPDATE bj_jobs SET locked_by = 'elsewhere' WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("id", Guid.Parse(id));
            await cmd.ExecuteNonQueryAsync();
        }

        await node.Api.WaitForEventsAsync(
            e => e.Any(x => x.JobId == id && x.Type == "message" && x.Detail?.GetProperty("message").GetString() == "cancelled"),
            TimeSpan.FromSeconds(3));
        Assert.Contains("lost its lease", node.Output);
    }

    [Fact]
    public async Task A_node_whose_clock_is_behind_does_not_have_its_running_job_recovered_by_another_node()
    {
        var db = await pg.CreateDatabaseAsync();
        // Ten seconds behind: a lease measured on this clock would look long expired to the other node.
        await using var behind = await DemoProcess.StartAsync(db, workers: 1,
            env: new Dictionary<string, string>(DemoProcess.ShortLease) { ["CLOCK_OFFSET_MS"] = "-10000" });

        var id = await behind.Api.EnqueueAsync("sleep", new { delayMs = 4000 }, new { maxAttempts = 3 });
        await behind.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        await using var other = await DemoProcess.StartAsync(db, workers: 1, env: DemoProcess.ShortLease);

        var job = await other.Api.WaitForStateAsync(id, "succeeded", TimeSpan.FromSeconds(15));
        Assert.Equal("succeeded", Assert.Single(job.AttemptHistory).Outcome);
        Assert.DoesNotContain(await other.Api.EventsAsync(), e => e.JobId == id);
    }
}
