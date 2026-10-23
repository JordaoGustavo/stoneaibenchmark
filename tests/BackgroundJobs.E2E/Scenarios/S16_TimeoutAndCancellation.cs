using System.Diagnostics;
using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S16_TimeoutAndCancellation(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Job_over_its_type_timeout_is_cancelled_and_retried_as_a_failure(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store),
            env: new Dictionary<string, string> { ["SLEEP_TIMEOUT_MS"] = "500" });

        var id = await demo.Api.EnqueueAsync("sleep", new { delayMs = 10_000 }, new { maxAttempts = 2, retryDelayMs = 100 });
        var job = await demo.Api.WaitForStateAsync(id, "failed", TimeSpan.FromSeconds(10));

        Assert.Equal(["failed", "failed"], job.AttemptHistory.Select(a => a.Outcome));
        Assert.All(job.AttemptHistory, a => Assert.Equal("timed out after 500 ms", a.Error));
        Assert.All(job.AttemptHistory, a => Assert.True(a.FinishedAt - a.StartedAt < TimeSpan.FromSeconds(3), $"attempt took {a.FinishedAt - a.StartedAt}"));
        Assert.Contains(job.History, h => h.Reason == "retry_scheduled");
        Assert.Contains(job.History, h => h.Reason == "max_attempts_exhausted");

        // The job code itself saw the token fire, once per attempt.
        var events = await demo.Api.EventsAsync();
        Assert.Equal([1, 2], events.Where(e => e.JobId == id && e.Type == "message").Select(e => e.Attempt));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Job_ignoring_the_token_frees_its_worker_but_keeps_the_job_until_its_code_returns(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1, env: new Dictionary<string, string>
        {
            ["JOB_TIMEOUT_MS"] = "500",
            ["TIMEOUT_GRACE_MS"] = "300",
            // Shorter than the stuck job: only lease renewal keeps another recovery from running it again.
            ["LEASE_MS"] = "900",
        });

        var stuck = await demo.Api.EnqueueAsync("sleep", new { delayMs = 3000, ignoreCancellation = true }, new { maxAttempts = 2, retryDelayMs = 0 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == stuck && x.Type == "started"));
        var next = await demo.Api.EnqueueAsync("log-message", new { message = "after the stuck one" });

        // With one worker, this only runs because the stuck job gave its slot back.
        var ran = await demo.Api.WaitForStateAsync(next, "succeeded", TimeSpan.FromSeconds(5));
        // The stuck job is still running, so it is still processing: no retry may start alongside it.
        Assert.Equal("processing", (await demo.Api.GetJobAsync(stuck)).State);

        // Its code finished the work despite the timeout: that is what gets recorded, once.
        var done = await demo.Api.WaitForStateAsync(stuck, "succeeded", TimeSpan.FromSeconds(10));
        var attempt = Assert.Single(done.AttemptHistory);
        Assert.True(ran.AttemptHistory[0].FinishedAt < attempt.FinishedAt);
    }

    [Fact]
    public async Task Abandoned_jobs_beyond_the_limit_keep_their_worker()
    {
        await using var demo = await DemoProcess.StartAsync(null, workers: 1, env: new Dictionary<string, string>
        {
            ["JOB_TIMEOUT_MS"] = "500",
            ["TIMEOUT_GRACE_MS"] = "300",
            ["MAX_ABANDONED_JOBS"] = "1",
        });

        var first = await demo.Api.EnqueueAsync("sleep", new { delayMs = 3000, ignoreCancellation = true });
        var second = await demo.Api.EnqueueAsync("sleep", new { delayMs = 3000, ignoreCancellation = true });
        var last = await demo.Api.EnqueueAsync("log-message", new { message = "waits for a free worker" });

        var a = await demo.Api.WaitForStateAsync(first, "succeeded", TimeSpan.FromSeconds(10));
        var b = await demo.Api.WaitForStateAsync(second, "succeeded", TimeSpan.FromSeconds(10));
        var c = await demo.Api.WaitForStateAsync(last, "succeeded", TimeSpan.FromSeconds(10));
        // The first was abandoned and freed the worker; the second could not be (limit 1) and held it.
        Assert.True(b.AttemptHistory[0].StartedAt < a.AttemptHistory[0].FinishedAt);
        Assert.True(c.AttemptHistory[0].StartedAt >= a.AttemptHistory[0].FinishedAt);
    }

    [Fact]
    public async Task Stop_cancels_cooperative_jobs_and_requeues_them_without_spending_an_attempt()
    {
        var db = await pg.CreateDatabaseAsync();
        var demo = await DemoProcess.StartAsync(db, workers: 1);

        // A single attempt: a deploy must still not fail the job, nor use up its budget.
        var id = await demo.Api.EnqueueAsync("sleep", new { delayMs = 3000 }, new { maxAttempts = 1 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));

        var stopping = Stopwatch.StartNew();
        demo.Terminate();
        await demo.WaitForExitAsync(TimeSpan.FromSeconds(10));
        Assert.True(stopping.Elapsed < TimeSpan.FromSeconds(2.5), $"stop took {stopping.Elapsed}");
        await demo.DisposeAsync();

        // Restarted under another node id: the job must not depend on lease expiry or own-node recovery.
        await using var restarted = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["NODE_ID"] = "other-node" });
        var job = await restarted.Api.WaitForStateAsync(id, "succeeded", TimeSpan.FromSeconds(10));
        Assert.Equal(["interrupted", "succeeded"], job.AttemptHistory.Select(a => a.Outcome));
        // Each execution keeps its own number; with maxAttempts 1, the second only ran because the first was given back.
        Assert.Equal([1, 2], job.AttemptHistory.Select(a => a.Attempt));
        Assert.Contains("1 of 1 (+1 interrupted by shutdown)", await (await restarted.Api.GetRawAsync($"/jobs/{id}")).Content.ReadAsStringAsync());
        Assert.Contains(job.History, h => h.Reason == "interrupted_by_shutdown");
        Assert.DoesNotContain(job.History, h => h.Reason == "recovered_after_crash");
    }

    [Fact]
    public async Task Stop_gives_up_on_jobs_ignoring_the_token_after_the_shutdown_timeout()
    {
        var db = await pg.CreateDatabaseAsync();
        var demo = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["SHUTDOWN_TIMEOUT_MS"] = "1000" });

        var id = await demo.Api.EnqueueAsync("sleep", new { delayMs = 30_000, ignoreCancellation = true }, new { maxAttempts = 1 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));

        var stopping = Stopwatch.StartNew();
        demo.Terminate();
        await demo.WaitForExitAsync(TimeSpan.FromSeconds(10));
        // Waited for the job, but only as long as allowed.
        Assert.InRange(stopping.Elapsed, TimeSpan.FromSeconds(0.9), TimeSpan.FromSeconds(4));
        await demo.DisposeAsync();

        await using var restarted = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["NODE_ID"] = "other-node" });
        var job = await restarted.Api.GetJobAsync(id);
        Assert.Equal("interrupted", job.AttemptHistory[0].Outcome);
        Assert.Equal(1, job.AttemptHistory[0].Attempt);
        Assert.Contains("(+1 interrupted by shutdown)", await (await restarted.Api.GetRawAsync($"/jobs/{id}")).Content.ReadAsStringAsync());
        Assert.Contains(job.History, h => h.Reason == "interrupted_by_shutdown");
        Assert.Contains(job.State, new[] { "enqueued", "processing" });
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Cancel_stops_waiting_and_running_jobs_for_good(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1);

        var waiting = await demo.Api.EnqueueAsync("log-message", new { message = "never" }, new { runAt = DateTime.UtcNow.AddMinutes(5) });
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.CancelAsync(waiting)).StatusCode);
        var cancelledWaiting = await demo.Api.GetJobAsync(waiting);
        Assert.Equal("cancelled", cancelledWaiting.State);
        Assert.Empty(cancelledWaiting.AttemptHistory);

        var running = await demo.Api.EnqueueAsync("sleep", new { delayMs = 30_000 }, new { maxAttempts = 3, retryDelayMs = 0 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == running && x.Type == "started"));
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.CancelAsync(running)).StatusCode);
        var cancelled = await demo.Api.WaitForStateAsync(running, "cancelled", TimeSpan.FromSeconds(3));
        Assert.Equal("cancelled", Assert.Single(cancelled.AttemptHistory).Outcome);
        Assert.Contains(cancelled.History, h => h.Reason == "cancelled_by_user");
        await Task.Delay(500);
        Assert.Single((await demo.Api.GetJobAsync(running)).AttemptHistory);

        // Finished jobs cannot be cancelled; cancelled ones can be requeued.
        Assert.Equal(HttpStatusCode.Conflict, (await demo.Api.CancelAsync(running)).StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.RequeueAsync(waiting)).StatusCode);
        await demo.Api.WaitForStateAsync(waiting, "succeeded");
    }

    [Fact]
    public async Task Cancel_reaches_a_job_running_on_another_node()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var runner = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string>
        {
            ["NODE_ID"] = "runner", ["LEASE_MS"] = "1500",
        });
        var id = await runner.Api.EnqueueAsync("sleep", new { delayMs = 30_000 });
        await runner.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));

        await using var other = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["NODE_ID"] = "other" });
        Assert.Equal(HttpStatusCode.Accepted, (await other.Api.CancelAsync(id)).StatusCode);

        var job = await other.Api.WaitForStateAsync(id, "cancelled", TimeSpan.FromSeconds(5));
        Assert.Equal("cancelled", Assert.Single(job.AttemptHistory).Outcome);
        Assert.Contains(await runner.Api.EventsAsync(), e => e.JobId == id && e.Type == "message");
    }

    [Fact]
    public async Task Cancel_is_not_lost_when_the_attempt_fails_before_the_owning_node_hears_of_it()
    {
        var db = await pg.CreateDatabaseAsync();
        // Default lease: the heartbeat that would deliver the cancel is ~10 s away; the attempt times out in 1.5 s.
        await using var runner = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string>
        {
            ["NODE_ID"] = "runner", ["SLEEP_TIMEOUT_MS"] = "1500",
        });
        var id = await runner.Api.EnqueueAsync("sleep", new { delayMs = 30_000 }, new { maxAttempts = 3, retryDelayMs = 0 });
        await runner.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));

        await using var other = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["NODE_ID"] = "other" });
        Assert.Equal(HttpStatusCode.Accepted, (await other.Api.CancelAsync(id)).StatusCode);

        var job = await other.Api.WaitForStateAsync(id, "cancelled", TimeSpan.FromSeconds(5));
        Assert.Equal("cancelled", Assert.Single(job.AttemptHistory).Outcome);
        Assert.DoesNotContain(job.History, h => h.Reason == "retry_scheduled");
    }
}
