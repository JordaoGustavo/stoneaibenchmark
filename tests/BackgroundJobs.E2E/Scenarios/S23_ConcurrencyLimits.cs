using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S23_ConcurrencyLimits(PostgresFixture pg)
{
    private static readonly Dictionary<string, string> LimitOne = new() { ["SUCCEED_AFTER_MAX_CONCURRENCY"] = "1" };

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_limited_job_never_exceeds_its_limit_while_other_jobs_run_past_it(string store)
    {
        await using var demo = await DemoProcess.StartAsync(
            await pg.CreateStoreAsync(store), workers: 4, env: new Dictionary<string, string> { ["SUCCEED_AFTER_MAX_CONCURRENCY"] = "2" });

        var limited = new List<string>();
        for (var i = 0; i < 6; i++) limited.Add(await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 400 }));
        var free = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 0 });

        var events = await demo.Api.WaitForEventsAsync(e => limited.All(id => e.Any(x => x.JobId == id && x.Type == "finished")));

        Assert.Equal(2, PeakRunning(events.Where(e => limited.Contains(e.JobId))));
        // The free job is claimed past the limited ones still enqueued, so it ends before the last of them starts.
        var freeFinished = Assert.Single(events, e => e.JobId == free && e.Type == "finished").At;
        Assert.True(freeFinished < events.Where(e => limited.Contains(e.JobId) && e.Type == "started").Max(e => e.At));
    }

    [Fact]
    public async Task The_limit_holds_across_nodes()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var a = await DemoProcess.StartAsync(db, workers: 4, env: LimitOne);
        await using var b = await DemoProcess.StartAsync(db, workers: 4, env: LimitOne);

        var ids = new List<string>();
        for (var i = 0; i < 4; i++) ids.Add(await a.Api.EnqueueAsync("succeed-after", new { delayMs = 300 }));
        foreach (var id in ids) await a.Api.WaitForStateAsync(id, "succeeded", TimeSpan.FromSeconds(20));

        // Each node records its own events; both run on this machine's clock, so merged by time they show the overlap.
        var events = (await a.Api.EventsAsync()).Concat(await b.Api.EventsAsync()).OrderBy(e => e.At);
        Assert.Equal(1, PeakRunning(events));
    }

    private static int PeakRunning(IEnumerable<EventView> events)
    {
        int running = 0, peak = 0;
        foreach (var e in events)
        {
            running += e.Type == "started" ? 1 : e.Type == "finished" ? -1 : 0;
            peak = Math.Max(peak, running);
        }
        return peak;
    }
}
