using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S04_WorkerLimit(PostgresFixture pg)
{
    [Fact]
    public async Task Two_workers_overlap_but_never_exceed_two_and_never_duplicate()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync(), workers: 2);

        var ids = new List<string>();
        for (var i = 0; i < 6; i++) ids.Add(await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 400 }));

        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "finished") == 6);

        // Events are recorded in order, so a running counter gives the concurrency at every instant.
        int running = 0, peak = 0;
        foreach (var e in events)
        {
            running += e.Type == "started" ? 1 : e.Type == "finished" ? -1 : 0;
            peak = Math.Max(peak, running);
        }
        Assert.Equal(2, peak);
        foreach (var id in ids)
            Assert.Single(events, e => e.JobId == id && e.Type == "started");
        foreach (var id in ids)
            Assert.Equal(1, (await demo.Api.WaitForStateAsync(id, "succeeded")).Attempts);
    }
}
