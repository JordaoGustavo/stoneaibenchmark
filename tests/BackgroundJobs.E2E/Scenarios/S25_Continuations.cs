using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S25_Continuations(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_continuation_runs_after_its_parent_succeeds(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var parent = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 1500 });
        var child = await demo.Api.EnqueueAsync("log-message", new { message = "after" }, new { continueAfter = parent });

        var awaiting = await demo.Api.GetJobAsync(child);
        Assert.Equal(("awaiting", parent, "on_succeeded"), (awaiting.State, awaiting.ContinueAfter, awaiting.ContinuationCondition));

        await demo.Api.WaitForStateAsync(child, "succeeded");
        var events = await demo.Api.EventsAsync();
        var parentFinished = events.Single(e => e.JobId == parent && e.Type == "finished").At;
        var childStarted = events.Single(e => e.JobId == child && e.Type == "started").At;
        Assert.True(childStarted >= parentFinished);
        Assert.Equal(["awaiting", "enqueued", "processing", "succeeded"], (await demo.Api.GetJobAsync(child)).History.Select(h => h.State));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_failed_parent_cancels_on_succeeded_continuations_and_runs_on_finished_ones(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var parent = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 5 }, new { maxAttempts = 1, runAt = DateTimeOffset.UtcNow.AddSeconds(1) });
        var onSucceeded = await demo.Api.EnqueueAsync("log-message", new { message = "never" }, new { continueAfter = parent });
        var onFinished = await demo.Api.EnqueueAsync("log-message", new { message = "cleanup" },
            new { continueAfter = parent, condition = "on_finished" });

        await demo.Api.WaitForStateAsync(parent, "failed");
        await demo.Api.WaitForStateAsync(onFinished, "succeeded");
        var skipped = await demo.Api.WaitForStateAsync(onSucceeded, "cancelled");
        Assert.Equal("continuation_condition_not_met", skipped.History[^1].Reason);
        Assert.DoesNotContain(await demo.Api.EventsAsync(), e => e.JobId == onSucceeded);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Continuations_of_a_finished_parent_are_decided_at_enqueue_and_unknown_parents_are_rejected(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var parent = await demo.Api.EnqueueAsync("log-message", new { message = "first" });
        await demo.Api.WaitForStateAsync(parent, "succeeded");
        var child = await demo.Api.EnqueueAsync("log-message", new { message = "second" }, new { continueAfter = parent });
        await demo.Api.WaitForStateAsync(child, "succeeded");

        var response = await demo.Api.PostJobRawAsync(new
        {
            job = "log-message", @params = new { message = "orphan" }, options = new { continueAfter = Guid.CreateVersion7() },
        });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Deleting_a_parent_cancels_its_awaiting_chain(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var a = await demo.Api.EnqueueAsync("log-message", new { message = "a" }, new { runAt = DateTimeOffset.UtcNow.AddMinutes(10) });
        var b = await demo.Api.EnqueueAsync("log-message", new { message = "b" }, new { continueAfter = a });
        var c = await demo.Api.EnqueueAsync("log-message", new { message = "c" }, new { continueAfter = b });

        Assert.Equal(HttpStatusCode.OK, (await demo.Api.DeleteAsync(a)).StatusCode);
        Assert.Equal("cancelled", (await demo.Api.GetJobAsync(b)).State);
        Assert.Equal("cancelled", (await demo.Api.GetJobAsync(c)).State);
    }
}
