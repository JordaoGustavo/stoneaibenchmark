using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S07_DeleteAndRequeue(PostgresFixture pg)
{
    [Fact]
    public async Task Deleted_job_never_runs_and_requeued_job_runs_again_with_history_kept()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());

        var scheduled = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddSeconds(1) });
        var delete = await demo.Api.DeleteAsync(scheduled);
        Assert.Equal(HttpStatusCode.OK, delete.StatusCode);
        Assert.Equal($$"""{"id":"{{scheduled}}","state":"deleted"}""", await delete.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.DeleteAsync(scheduled)).StatusCode);

        var finished = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        var before = await demo.Api.WaitForStateAsync(finished, "succeeded");
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.DeleteAsync(finished)).StatusCode);

        var requeue = await demo.Api.RequeueAsync(finished);
        Assert.Equal(HttpStatusCode.Accepted, requeue.StatusCode);
        Assert.Equal($$"""{"jobId":"{{finished}}"}""", await requeue.Content.ReadAsStringAsync());

        var after = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.JobId == finished && x.Type == "finished") == 2)
            .ContinueWith(_ => demo.Api.WaitForStateAsync(finished, "succeeded")).Unwrap();
        Assert.Equal(1, after.Attempts);
        Assert.Equal([1, 1], after.AttemptHistory.Select(a => a.Attempt));
        Assert.Equal(before.History, after.History.Take(before.History.Count));
        Assert.Equal(["requeued_by_user", "claimed", "succeeded"], after.History.Skip(before.History.Count).Select(h => h.Reason));

        // Past the deleted job's runAt it still has not run.
        await Task.Delay(1500);
        Assert.DoesNotContain(await demo.Api.EventsAsync(), e => e.JobId == scheduled);
        Assert.Equal("deleted", (await demo.Api.GetJobAsync(scheduled)).State);

        // Requeue only from final states.
        var pending = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddMinutes(5) });
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.RequeueAsync(pending)).StatusCode);
        // A deleted job can be requeued.
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.RequeueAsync(scheduled)).StatusCode);
        await demo.Api.WaitForStateAsync(scheduled, "succeeded");
    }

    [Fact]
    public async Task Unknown_ids_return_404()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        var missing = Guid.CreateVersion7().ToString();

        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetJobRawAsync(missing)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.DeleteAsync(missing)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.RequeueAsync(missing)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetJobRawAsync("not-a-guid")).StatusCode);
    }
}
