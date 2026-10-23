using System.Net;
using System.Text.RegularExpressions;
using BackgroundJobs.E2E.Infra;
using Npgsql;

namespace BackgroundJobs.E2E.Scenarios;

/// Finished jobs (succeeded, failed, deleted) are purged, with their history and attempts, once older than the
/// retention the integrator configured. Unfinished jobs are never purged.
[Collection(E2ECollection.Name)]
public sealed class S15_Retention(PostgresFixture pg)
{
    private static readonly Dictionary<string, string> ShortRetention = new() { ["RETENTION_SECONDS"] = "2", ["RETENTION_SWEEP_MS"] = "200" };

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Finished_jobs_are_purged_after_retention_and_unfinished_ones_are_kept(string store)
    {
        var db = await pg.CreateStoreAsync(store);
        await using var demo = await DemoProcess.StartAsync(db, env: ShortRetention);

        var succeeded = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        var failed = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 5 }, new { maxAttempts = 1 });
        var deleted = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddHours(1) });
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.DeleteAsync(deleted)).StatusCode);
        var scheduled = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddHours(1) });
        await demo.Api.WaitForStateAsync(succeeded, "succeeded");
        await demo.Api.WaitForStateAsync(failed, "failed");

        // Still within retention.
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.GetJobRawAsync(succeeded)).StatusCode);

        foreach (var id in new[] { succeeded, failed, deleted }) await WaitGoneAsync(demo.Api, id);
        Assert.Equal("scheduled", (await demo.Api.GetJobAsync(scheduled)).State);

        if (db != null)
        {
            await using var conn = new NpgsqlConnection(db);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT (SELECT count(*) FROM bj_history WHERE job_id <> @keep) + (SELECT count(*) FROM bj_attempts WHERE job_id <> @keep)", conn);
            cmd.Parameters.AddWithValue("keep", Guid.Parse(scheduled));
            Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync())!);
        }
    }

    [Fact]
    public async Task Server_refuses_to_start_when_retention_is_not_configured()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DemoProcess.StartAsync(connectionString: null, env: new Dictionary<string, string> { ["RETENTION_SECONDS"] = "unset" }));
        Assert.Contains("FinishedJobRetention", ex.Message);
    }

    [Fact]
    public async Task Infinite_retention_keeps_finished_jobs()
    {
        await using var demo = await DemoProcess.StartAsync(connectionString: null,
            env: new Dictionary<string, string> { ["RETENTION_SECONDS"] = "forever", ["RETENTION_SWEEP_MS"] = "200" });
        var id = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        await demo.Api.WaitForStateAsync(id, "succeeded");
        await Task.Delay(1000);
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.GetJobRawAsync(id)).StatusCode);
    }

    [Fact]
    public async Task Two_nodes_sweeping_together_purge_each_job_exactly_once()
    {
        var db = await pg.CreateDatabaseAsync();
        // Small batches and a fast sweep so both nodes are purging at the same time.
        var env = new Dictionary<string, string> { ["RETENTION_SECONDS"] = "1", ["RETENTION_SWEEP_MS"] = "50", ["RETENTION_BATCH"] = "5" };
        await using var a = await DemoProcess.StartAsync(db, env: new Dictionary<string, string>(env) { ["NODE_ID"] = "a" });
        await using var b = await DemoProcess.StartAsync(db, env: new Dictionary<string, string>(env) { ["NODE_ID"] = "b" });

        // Sequential: 200 parallel requests would exhaust Postgres connections, which is not what this tests.
        var ids = new List<string>();
        for (var i = 0; i < 200; i++) ids.Add(await (i % 2 == 0 ? a : b).Api.EnqueueAsync("succeed-after", new { delayMs = 0 }));
        foreach (var id in ids) await WaitGoneAsync(a.Api, id);

        Assert.Equal(0L, await CountAsync(db, "SELECT (SELECT count(*) FROM bj_jobs) + (SELECT count(*) FROM bj_history) + (SELECT count(*) FROM bj_attempts)"));
        // Every job was counted by exactly one node: no double delete, no error from a node losing the race.
        Assert.Equal(ids.Count, Purged(a) + Purged(b));
        Assert.True(Purged(a) > 0 && Purged(b) > 0, $"both nodes should purge: a={Purged(a)} b={Purged(b)}");
        Assert.DoesNotContain("fail:", a.Output + b.Output);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Requeue_racing_the_purge_either_revives_the_job_or_finds_it_gone(string store)
    {
        var db = await pg.CreateStoreAsync(store);
        await using var demo = await DemoProcess.StartAsync(db,
            env: new Dictionary<string, string> { ["RETENTION_SECONDS"] = "1", ["RETENTION_SWEEP_MS"] = "20" });

        // Each run takes 300ms, so a burst's later requests find the revived job still running (Conflict).
        var ids = await Task.WhenAll(Enumerable.Range(0, 40).Select(_ => demo.Api.EnqueueAsync("succeed-after", new { delayMs = 300 })));

        // Per job, a burst of concurrent requeues at a random moment around its retention deadline (1s after it finished).
        var bursts = await Task.WhenAll(ids.Select(async id =>
        {
            await demo.Api.WaitForStateAsync(id, "succeeded");
            await Task.Delay(Random.Shared.Next(700, 1300));
            var burst = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => demo.Api.RequeueAsync(id)));
            return burst.Select(r => r.StatusCode).ToList();
        }));

        // Accepted (revived), Conflict (a sibling request revived it first) or NotFound (purged): never a server error.
        Assert.All(bursts.SelectMany(b => b), s => Assert.Contains(s, new[] { HttpStatusCode.Accepted, HttpStatusCode.Conflict, HttpStatusCode.NotFound }));
        // At most one request per burst revives the job.
        Assert.All(bursts, b => Assert.True(b.Count(s => s == HttpStatusCode.Accepted) <= 1, string.Join(",", b)));
        // The timing spread makes both outcomes happen; otherwise the test did not exercise the race.
        Assert.Contains(bursts, b => b.Contains(HttpStatusCode.Accepted));
        Assert.Contains(bursts, b => b.All(s => s == HttpStatusCode.NotFound));

        // Revived jobs run again and are purged in turn; nothing is left behind.
        foreach (var id in ids) await WaitGoneAsync(demo.Api, id);
        Assert.DoesNotContain("fail:", demo.Output);
        if (db != null)
            Assert.Equal(0L, await CountAsync(db, "SELECT (SELECT count(*) FROM bj_jobs) + (SELECT count(*) FROM bj_history) + (SELECT count(*) FROM bj_attempts)"));
    }

    private static int Purged(DemoProcess node) =>
        Regex.Matches(node.Output, @"purged (\d+) finished jobs").Sum(m => int.Parse(m.Groups[1].Value));

    private static async Task<long> CountAsync(string db, string sql)
    {
        await using var conn = new NpgsqlConnection(db);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task WaitGoneAsync(ApiClient api, string id)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while ((await api.GetJobRawAsync(id)).StatusCode != HttpStatusCode.NotFound)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"job {id} was never purged");
            await Task.Delay(100);
        }
    }
}
