using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S02_Scheduling(PostgresFixture pg)
{
    private static readonly TimeSpan StartTolerance = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Future_job_waits_for_runAt_and_past_job_runs_immediately()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());

        var runAt = DateTime.UtcNow.AddSeconds(3);
        var future = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt });
        var scheduled = await demo.Api.GetJobAsync(future);
        Assert.Equal("scheduled", scheduled.State);
        Assert.Equal(runAt, scheduled.ScheduledAt!.Value, TimeSpan.FromMilliseconds(1));
        Assert.Equal("scheduled_at_creation", Assert.Single(scheduled.History).Reason);

        var pastEnqueuedAt = DateTime.UtcNow;
        var past = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddMinutes(-5) });

        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "started") == 2);
        var futureStart = events.Single(e => e.JobId == future && e.Type == "started").At;
        var pastStart = events.Single(e => e.JobId == past && e.Type == "started").At;

        Assert.True(futureStart >= runAt, $"started {futureStart:O} before runAt {runAt:O}");
        Assert.True(futureStart - runAt < StartTolerance, $"started {futureStart - runAt} after runAt");
        Assert.True(pastStart - pastEnqueuedAt < StartTolerance);

        var done = await demo.Api.WaitForStateAsync(future, "succeeded");
        Assert.Equal(["scheduled", "enqueued", "processing", "succeeded"], done.History.Select(h => h.State));
        Assert.Equal("became_due", done.History[1].Reason);
        Assert.Null(done.ScheduledAt);
        Assert.Equal(["enqueued", "processing", "succeeded"], (await demo.Api.WaitForStateAsync(past, "succeeded")).History.Select(h => h.State));
    }
}
