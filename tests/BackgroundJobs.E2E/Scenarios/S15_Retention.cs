using System.Net;
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
