using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// Nodes sharing a database with different registrations, e.g. in a rolling deploy: a node never claims a job whose name it
/// has not registered, so the job waits for a node that has it instead of failing as an unknown job.
[Collection(E2ECollection.Name)]
public sealed class S24_UnregisteredJobNames(PostgresFixture pg)
{
    private static readonly Dictionary<string, string> OldNode = new()
    {
        ["UNREGISTERED_JOBS"] = "log-message", ["QUEUE_MONITOR_MS"] = "100", ["STALLED_QUEUE_SECONDS"] = "1",
    };

    [Fact]
    public async Task A_job_no_live_node_registered_stays_enqueued_and_stalls_the_queue_until_a_node_that_has_it_starts()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var old = await DemoProcess.StartAsync(db, env: OldNode);

        var waiting = await old.Api.EnqueueAsync("log-message", new { message = "from the new deploy" });
        var runnable = await old.Api.EnqueueAsync("fail-n-times", new { failTimes = 0 });
        await old.Api.WaitForStateAsync(runnable, "succeeded");

        // Visible as a stalled queue on the node that cannot run it, and never claimed: no attempt spent.
        var health = await WaitForHealthAsync(old, h => h["queue"]!["stalled"]!.GetValue<bool>());
        Assert.Equal(1, health["queue"]!["ready"]!.GetValue<int>());
        var job = await old.Api.GetJobAsync(waiting);
        Assert.Equal("enqueued", job.State);
        Assert.Equal(0, job.Attempts);
        Assert.Empty(job.AttemptHistory);

        await using var updated = await DemoProcess.StartAsync(db);
        job = await updated.Api.WaitForStateAsync(waiting, "succeeded", TimeSpan.FromSeconds(20));
        Assert.Equal("succeeded", Assert.Single(job.AttemptHistory).Outcome);
        Assert.DoesNotContain(await old.Api.EventsAsync(), e => e.JobId == waiting);
    }

    private static async Task<JsonNode> WaitForHealthAsync(DemoProcess demo, Func<JsonNode, bool> until)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (true)
        {
            var res = await demo.Api.GetRawAsync("/health/details");
            var json = JsonNode.Parse(await res.Content.ReadAsStringAsync())!;
            if (until(json) || DateTime.UtcNow > deadline) return json;
            await Task.Delay(100);
        }
    }
}
