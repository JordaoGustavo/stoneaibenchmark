using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S10_GracefulStop(PostgresFixture pg)
{
    [Fact]
    public async Task Stop_waits_for_running_job_and_does_not_start_pending_one()
    {
        var db = await pg.CreateDatabaseAsync();
        var demo = await DemoProcess.StartAsync(db, workers: 1);

        var running = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == running && x.Type == "started"));
        var pending = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });

        demo.Terminate();
        await Task.Delay(200);
        var health = await demo.Api.GetHealthAsync();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, health.StatusCode);
        Assert.Equal("""{"status":"unavailable"}""", await health.Content.ReadAsStringAsync());
        // HTTP keeps accepting work while the server drains.
        var acceptedDuringStop = await demo.Api.EnqueueAsync("log-message", new { message = "queued during stop" });

        Assert.DoesNotContain(await demo.Api.EventsAsync(), e => e.JobId == pending);
        // The process exits only after the running job finished.
        await demo.WaitForExitAsync(TimeSpan.FromSeconds(10));
        var exitedAt = DateTime.UtcNow;

        await using var restarted = await DemoProcess.StartAsync(db, workers: 1);
        var first = await restarted.Api.GetJobAsync(running);
        Assert.Equal("succeeded", first.State);
        Assert.Equal("succeeded", Assert.Single(first.AttemptHistory).Outcome);
        Assert.True(first.UpdatedAt < exitedAt);

        // The pending job was not started during the stop: its only attempt began after the restart.
        var second = await restarted.Api.WaitForStateAsync(pending, "succeeded");
        Assert.True(Assert.Single(second.AttemptHistory).StartedAt > exitedAt);
        await restarted.Api.WaitForStateAsync(acceptedDuringStop, "succeeded");
        await demo.DisposeAsync();
    }
}
