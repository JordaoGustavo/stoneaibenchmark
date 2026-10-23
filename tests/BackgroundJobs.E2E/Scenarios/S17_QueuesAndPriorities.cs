using System.Net.Http.Json;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S17_QueuesAndPriorities(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Higher_priority_runs_first_and_equal_priority_stays_fifo(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1,
            env: new Dictionary<string, string> { ["FIFO_CLAIM_EVERY"] = "off" });

        // Holds the only worker while the rest are enqueued, so they are all ready when it frees up.
        var blocker = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 800 });
        await demo.Api.WaitForStateAsync(blocker, "processing");
        foreach (var (message, priority) in new[] { ("A", 0), ("B", 10), ("C", 0), ("D", 10), ("E", -5) })
            await demo.Api.EnqueueAsync("log-message", new { message }, new { priority });

        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "message") == 5);
        Assert.Equal(["B", "D", "A", "C", "E"], events.Where(e => e.Type == "message").Select(e => e.Detail!.Value.GetProperty("message").GetString()));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_busy_queue_does_not_hold_up_another_queue(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store),
            env: new Dictionary<string, string> { ["QUEUES"] = "default=1,critical=1" });

        var slow = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 });
        var queued = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        await demo.Api.WaitForStateAsync(slow, "processing");
        var urgent = await demo.Api.EnqueueAsync("log-message", new { message = "urgent" }, new { queue = "critical" });

        var job = await demo.Api.WaitForStateAsync(urgent, "succeeded");
        Assert.Equal("critical", job.Queue);
        // default has one worker, busy with the slow job: the second default job still waits behind it.
        Assert.Equal("processing", (await demo.Api.GetJobAsync(slow)).State);
        Assert.Equal("enqueued", (await demo.Api.GetJobAsync(queued)).State);
        Assert.Equal("default", (await demo.Api.GetJobAsync(queued)).Queue);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_queue_no_server_consumes_keeps_its_jobs_enqueued(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var id = await demo.Api.EnqueueAsync("log-message", new { message = "later" }, new { queue = "reports", priority = 3 });
        var served = await demo.Api.EnqueueAsync("log-message", new { message = "now" });

        await demo.Api.WaitForStateAsync(served, "succeeded");
        await Task.Delay(500);
        var job = await demo.Api.GetJobAsync(id);
        Assert.Equal(("enqueued", "reports", 3), (job.State, job.Queue, job.Priority));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Every_nth_claim_takes_the_oldest_job_so_low_priorities_are_not_starved(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1,
            env: new Dictionary<string, string> { ["FIFO_CLAIM_EVERY"] = "3" });

        var blocker = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 800 });
        await demo.Api.WaitForStateAsync(blocker, "processing");
        await demo.Api.EnqueueAsync("log-message", new { message = "low" }, new { priority = -5 });
        foreach (var message in new[] { "H1", "H2", "H3", "H4", "H5" })
            await demo.Api.EnqueueAsync("log-message", new { message }, new { priority = 10 });

        // Claims: blocker, H1, then the 3rd takes the oldest ready job whatever its priority.
        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "message") == 6);
        Assert.Equal(["H1", "low", "H2", "H3", "H4", "H5"],
            events.Where(e => e.Type == "message").Select(e => e.Detail!.Value.GetProperty("message").GetString()));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_queue_no_node_consumes_degrades_health_once_its_jobs_wait_too_long(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store),
            env: new Dictionary<string, string> { ["QUEUE_MONITOR_MS"] = "100", ["STALLED_QUEUE_SECONDS"] = "0.5" });

        await demo.Api.EnqueueAsync("log-message", new { message = "typo" }, new { queue = "critcal" });

        var health = await WaitForHealthAsync(demo, h => h["status"]!.GetValue<string>() == "degraded");
        var queue = health["queue"]!["queues"]!.AsArray().Single(q => q!["name"]!.GetValue<string>() == "critcal")!;
        Assert.Equal((false, true), (queue["consumed"]!.GetValue<bool>(), queue["orphaned"]!.GetValue<bool>()));
        // Only the queues this node consumes count as stalled; the orphan is reported on its own.
        Assert.False(health["queue"]!["stalled"]!.GetValue<bool>());
    }

    [Fact]
    public async Task A_queue_consumed_by_another_node_is_not_orphaned()
    {
        var db = await pg.CreateDatabaseAsync();
        var env = new Dictionary<string, string> { ["QUEUE_MONITOR_MS"] = "100", ["STALLED_QUEUE_SECONDS"] = "0.5" };
        await using var web = await DemoProcess.StartAsync(db, env: new Dictionary<string, string>(env) { ["NODE_ID"] = "web" });
        await using var reports = await DemoProcess.StartAsync(db, env: new Dictionary<string, string>(env) { ["NODE_ID"] = "reports", ["QUEUES"] = "reports=1" });

        // The reports node's only worker is busy, so the second reports job waits past the threshold.
        var slow = await web.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 }, new { queue = "reports" });
        await web.Api.WaitForStateAsync(slow, "processing");
        await web.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { queue = "reports" });

        var health = await WaitForHealthAsync(web, h => h["queue"]!["queues"]!.AsArray()
            .Any(q => q!["name"]!.GetValue<string>() == "reports" && q["oldestReadyAgeSeconds"]!.GetValue<double>() > 0.5));
        Assert.Equal("healthy", health["status"]!.GetValue<string>());
        Assert.False(health["queue"]!["queues"]!.AsArray().Single(q => q!["name"]!.GetValue<string>() == "reports")!["orphaned"]!.GetValue<bool>());
        Assert.Equal("degraded", (await WaitForHealthAsync(reports, h => h["status"]!.GetValue<string>() == "degraded"))["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task A_node_that_stops_gracefully_stops_counting_as_a_consumer_at_once()
    {
        var db = await pg.CreateDatabaseAsync();
        var env = new Dictionary<string, string> { ["QUEUE_MONITOR_MS"] = "100", ["STALLED_QUEUE_SECONDS"] = "0.5" };
        await using var web = await DemoProcess.StartAsync(db, env: env);
        var reports = await DemoProcess.StartAsync(db, env: new Dictionary<string, string>(env) { ["QUEUES"] = "reports=1" });
        reports.Terminate();
        await reports.WaitForExitAsync(TimeSpan.FromSeconds(10));

        // Without deregistering, the stopped node would still count until its 30 s registration lapsed.
        await web.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { queue = "reports" });
        var health = await WaitForHealthAsync(web, h => h["queue"]!["queues"]!.AsArray()
            .Any(q => q!["name"]!.GetValue<string>() == "reports" && q["orphaned"]!.GetValue<bool>()));
        Assert.Equal("degraded", health["status"]!.GetValue<string>());
    }

    private static async Task<JsonObject> WaitForHealthAsync(DemoProcess demo, Func<JsonObject, bool> until)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        JsonObject health;
        do
        {
            health = (await (await demo.Api.GetRawAsync("/health/details")).Content.ReadFromJsonAsync<JsonObject>())!;
            if (until(health)) return health;
            await Task.Delay(100);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException("health condition not met: " + health.ToJsonString());
    }
}
