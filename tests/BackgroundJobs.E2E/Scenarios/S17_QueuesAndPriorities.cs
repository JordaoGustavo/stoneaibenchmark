using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S17_QueuesAndPriorities(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Higher_priority_runs_first_and_equal_priority_stays_fifo(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1);

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
}
