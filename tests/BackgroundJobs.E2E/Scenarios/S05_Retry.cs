using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S05_Retry(PostgresFixture pg)
{
    [Fact]
    public async Task Fails_twice_then_succeeds_respecting_the_retry_delay()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());

        var id = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 2 }, new { maxAttempts = 3, retryDelayMs = 500 });
        var job = await demo.Api.WaitForStateAsync(id, "succeeded");

        Assert.Equal(3, job.Attempts);
        Assert.Equal([1, 2, 3], job.AttemptHistory.Select(a => a.Attempt));
        Assert.Equal(["failed", "failed", "succeeded"], job.AttemptHistory.Select(a => a.Outcome));
        Assert.Equal(["demo failure", "demo failure", null], job.AttemptHistory.Select(a => a.Error));
        for (var i = 1; i < job.AttemptHistory.Count; i++)
        {
            var gap = job.AttemptHistory[i].StartedAt - job.AttemptHistory[i - 1].FinishedAt!.Value;
            Assert.True(gap >= TimeSpan.FromMilliseconds(500), $"gap before attempt {i + 1} was {gap}");
        }
        Assert.Equal(2, job.History.Count(h => h.Reason == "retry_scheduled"));

        var events = await demo.Api.EventsAsync();
        Assert.Equal([1, 2, 3], events.Where(e => e.JobId == id && e.Type == "started").Select(e => e.Attempt));
    }
}
