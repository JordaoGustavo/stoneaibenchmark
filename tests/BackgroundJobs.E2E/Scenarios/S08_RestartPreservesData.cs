using System.Text.Json;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S08_RestartPreservesData(PostgresFixture pg)
{
    [Fact]
    public async Task Kill_and_restart_keeps_everything_and_scheduled_jobs_still_run()
    {
        var db = await pg.CreateDatabaseAsync();
        var demo = await DemoProcess.StartAsync(db);

        var finished = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 1 }, new { retryDelayMs = 0 });
        var before = await demo.Api.WaitForStateAsync(finished, "succeeded");
        var scheduled = await demo.Api.EnqueueAsync("log-message", new { message = "after restart" }, new { runAt = DateTime.UtcNow.AddSeconds(3) });
        var scheduledBefore = await demo.Api.GetJobAsync(scheduled);

        await demo.KillAsync();
        await using var restarted = await DemoProcess.StartAsync(db);

        var finishedAfter = await restarted.Api.GetJobAsync(finished);
        Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(finishedAfter));

        var scheduledAfter = await restarted.Api.GetJobAsync(scheduled);
        Assert.Equal("scheduled", scheduledAfter.State);
        Assert.Equal(scheduledBefore.ScheduledAt, scheduledAfter.ScheduledAt);
        Assert.Equal("after restart", scheduledAfter.Args.GetProperty("message").GetString());

        var done = await restarted.Api.WaitForStateAsync(scheduled, "succeeded");
        Assert.Equal(scheduledBefore.History, done.History.Take(1));
        await restarted.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == scheduled && x.Type == "message"));
    }
}
