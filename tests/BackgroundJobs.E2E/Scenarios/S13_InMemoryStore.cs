using System.Net;
using System.Text.Json;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// The demo with UseInMemoryStorage(): no Postgres at all, same observable behaviour through the same API.
[Collection(E2ECollection.Name)]
public sealed class S13_InMemoryStore
{
    [Fact]
    public async Task Runs_retries_schedules_deletes_and_requeues_without_postgres()
    {
        await using var demo = await DemoProcess.StartAsync(connectionString: null);

        var ok = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        var succeeded = await demo.Api.WaitForStateAsync(ok, "succeeded");
        Assert.Equal(["created", "claimed", "succeeded"], succeeded.History.Select(h => h.Reason));
        Assert.Equal(["enqueued", "processing", "succeeded"], succeeded.History.Select(h => h.State));

        var retried = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 1 }, new { retryDelayMs = 200 });
        var afterRetry = await demo.Api.WaitForStateAsync(retried, "succeeded");
        Assert.Equal(2, afterRetry.Attempts);
        Assert.Equal(["failed", "succeeded"], afterRetry.AttemptHistory.Select(a => a.Outcome));
        Assert.Equal("demo failure", afterRetry.AttemptHistory[0].Error);
        Assert.Equal(["created", "claimed", "retry_scheduled", "became_due", "claimed", "succeeded"], afterRetry.History.Select(h => h.Reason));

        var exhausted = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 5 }, new { maxAttempts = 1 });
        Assert.Equal("max_attempts_exhausted", (await demo.Api.WaitForStateAsync(exhausted, "failed")).History[^1].Reason);

        var scheduled = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddSeconds(1) });
        var pending = await demo.Api.GetJobAsync(scheduled);
        Assert.Equal("scheduled", pending.State);
        Assert.NotNull(pending.ScheduledAt);
        Assert.Equal(["scheduled_at_creation", "became_due", "claimed", "succeeded"],
            (await demo.Api.WaitForStateAsync(scheduled, "succeeded")).History.Select(h => h.Reason));

        var later = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTime.UtcNow.AddMinutes(5) });
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.RequeueAsync(later)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.DeleteAsync(later)).StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.DeleteAsync(later)).StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.RequeueAsync(later)).StatusCode);
        var requeued = await demo.Api.WaitForStateAsync(later, "succeeded");
        Assert.Equal(["scheduled_at_creation", "deleted_by_user", "requeued_by_user", "claimed", "succeeded"], requeued.History.Select(h => h.Reason));

        var missing = Guid.CreateVersion7().ToString();
        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetJobRawAsync(missing)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.DeleteAsync(missing)).StatusCode);

        // Listing pages in creation order, like the Postgres store.
        var created = new[] { ok, retried, exhausted, scheduled, later };
        var first = JsonDocument.Parse(await (await demo.Api.ListRawAsync("?limit=3")).Content.ReadAsStringAsync()).RootElement;
        var cursor = first.GetProperty("nextCursor").GetString();
        var second = JsonDocument.Parse(await (await demo.Api.ListRawAsync($"?limit=3&after={cursor}")).Content.ReadAsStringAsync()).RootElement;
        var ids = first.GetProperty("items").EnumerateArray().Concat(second.GetProperty("items").EnumerateArray())
            .Select(i => i.GetProperty("id").GetString()).ToList();
        Assert.Equal(created, ids);
    }

    [Fact]
    public async Task Jobs_run_in_fifo_order_on_a_single_worker()
    {
        await using var demo = await DemoProcess.StartAsync(connectionString: null, workers: 1);
        var blocker = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 300 });
        var ids = new List<string>();
        for (var i = 0; i < 5; i++) ids.Add(await demo.Api.EnqueueAsync("log-message", new { message = $"m{i}" }));

        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "message") == 5);
        Assert.Equal(ids, events.Where(e => e.Type == "message").Select(e => e.JobId));
        Assert.Equal(blocker, events[0].JobId);
    }

    [Fact]
    public async Task Dashboard_package_renders_jobs()
    {
        await using var demo = await DemoProcess.StartAsync(connectionString: null);
        var id = await demo.Api.EnqueueAsync("log-message", new { message = "hi" });
        await demo.Api.WaitForStateAsync(id, "succeeded");

        var list = await demo.Api.GetRawAsync("/jobs");
        Assert.Equal(HttpStatusCode.OK, list.StatusCode);
        Assert.Contains("log-message", await list.Content.ReadAsStringAsync());
        var detail = await demo.Api.GetRawAsync($"/jobs/{id}");
        Assert.Equal(HttpStatusCode.OK, detail.StatusCode);
        Assert.Contains(id, await detail.Content.ReadAsStringAsync());
    }
}
