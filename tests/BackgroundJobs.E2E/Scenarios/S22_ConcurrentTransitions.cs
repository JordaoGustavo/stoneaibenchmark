using System.Net;
using BackgroundJobs.E2E.Infra;
using Npgsql;

namespace BackgroundJobs.E2E.Scenarios;

/// What the store does while several actors touch the same jobs: the scheduler promoting, a user deleting or
/// requeueing, the retention sweep purging.
[Collection(E2ECollection.Name)]
public sealed class S22_ConcurrentTransitions(PostgresFixture pg)
{
    /// Jobs nobody consumes stay where the test put them.
    private static readonly object Unconsumed = new { queue = "nobody" };

    [Fact]
    public async Task Jobs_due_together_are_enqueued_in_run_at_order()
    {
        var db = await pg.CreateDatabaseAsync();
        var first = await DemoProcess.StartAsync(db);
        // Created latest-due first, so creation order and run_at order disagree.
        var dueAt = DateTime.UtcNow.AddSeconds(2);
        var ids = new List<string>();
        for (var i = 0; i < 8; i++)
            ids.Add(await first.Api.EnqueueAsync("log-message", new { message = $"m{i}" },
                new { queue = "nobody", runAt = dueAt.AddMilliseconds(-100 * i) }));
        // Down while they become due, so the next start promotes them all in one pass.
        await first.KillAsync();
        await first.DisposeAsync();
        await Task.Delay(dueAt - DateTime.UtcNow + TimeSpan.FromMilliseconds(300));

        await using var second = await DemoProcess.StartAsync(db);
        foreach (var id in ids) await second.Api.WaitForStateAsync(id, "enqueued");

        await using var conn = new NpgsqlConnection(db);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT id FROM bj_jobs ORDER BY enqueue_seq", conn);
        var bySequence = new List<string>();
        await using (var reader = await cmd.ExecuteReaderAsync())
            while (await reader.ReadAsync()) bySequence.Add(reader.GetGuid(0).ToString());
        ids.Reverse();
        Assert.Equal(ids, bySequence);
    }

    [Fact]
    public async Task Deleting_jobs_while_they_are_promoted_never_reports_a_conflict()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);
        var dueAt = DateTime.UtcNow.AddSeconds(2);
        var ids = new List<string>();
        for (var i = 0; i < 60; i++)
            ids.Add(await demo.Api.EnqueueAsync("log-message", new { message = "x" }, new { queue = "nobody", runAt = dueAt }));

        // Spread around the moment they become due: some deletes land on scheduled jobs, some on enqueued ones, some
        // while the promotion holds their rows. Both states can be deleted, so every delete must succeed.
        await Task.Delay(dueAt - DateTime.UtcNow - TimeSpan.FromMilliseconds(150));
        var responses = await Task.WhenAll(ids.Select(async (id, i) =>
        {
            await Task.Delay(i * 5);
            return (await demo.Api.DeleteAsync(id)).StatusCode;
        }));

        Assert.All(responses, s => Assert.Equal(HttpStatusCode.OK, s));
        foreach (var id in ids) Assert.Equal("deleted", (await demo.Api.GetJobAsync(id)).State);
    }

    [Fact]
    public async Task Requeueing_jobs_while_retention_purges_them_reports_not_found_never_a_conflict()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db,
            env: new Dictionary<string, string> { ["RETENTION_SECONDS"] = "1", ["RETENTION_SWEEP_MS"] = "20" });
        var ids = new List<string>();
        for (var i = 0; i < 60; i++)
        {
            var id = await demo.Api.EnqueueAsync("log-message", new { message = "x" }, Unconsumed);
            Assert.Equal(HttpStatusCode.OK, (await demo.Api.DeleteAsync(id)).StatusCode);
            ids.Add(id);
        }

        // Spread over the time they become purgeable: each one is requeued once, before, during or after its purge.
        var responses = await Task.WhenAll(ids.Select(async (id, i) =>
        {
            await Task.Delay(800 + i * 10);
            return (await demo.Api.RequeueAsync(id)).StatusCode;
        }));

        Assert.All(responses, s => Assert.Contains(s, new[] { HttpStatusCode.Accepted, HttpStatusCode.NotFound }));
        Assert.Contains(HttpStatusCode.NotFound, responses);
    }
}
