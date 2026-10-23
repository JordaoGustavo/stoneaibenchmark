using System.Diagnostics;
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

    [Fact]
    public async Task Job_ignoring_the_token_is_abandoned_after_the_grace_period_and_frees_its_worker()
    {
        await using var demo = await DemoProcess.StartAsync(null, workers: 1, env: new Dictionary<string, string>
        {
            ["JOB_TIMEOUT_MS"] = "500",
            ["TIMEOUT_GRACE_MS"] = "300",
        });

        var stuck = await demo.Api.EnqueueAsync("sleep", new { delayMs = 15_000, ignoreCancellation = true }, new { maxAttempts = 1 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == stuck && x.Type == "started"));
        var next = await demo.Api.EnqueueAsync("log-message", new { message = "after the stuck one" });

        var failed = await demo.Api.WaitForStateAsync(stuck, "failed", TimeSpan.FromSeconds(5));
        Assert.Equal("timed out after 500 ms and did not stop within 300 ms", Assert.Single(failed.AttemptHistory).Error);
        // With one worker, this only runs because the stuck job gave its slot back.
        await demo.Api.WaitForStateAsync(next, "succeeded", TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(await demo.Api.EventsAsync(), e => e.JobId == stuck && e.Type == "finished");
    }

    [Fact]
    public async Task Stop_cancels_cooperative_jobs_and_requeues_them_at_once_even_on_their_last_attempt()
    {
        var db = await pg.CreateDatabaseAsync();
        var demo = await DemoProcess.StartAsync(db, workers: 1);

        // A single attempt: a deploy must still not fail the job for good.
        var id = await demo.Api.EnqueueAsync("sleep", new { delayMs = 30_000 }, new { maxAttempts = 1 });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));

        var stopping = Stopwatch.StartNew();
        demo.Terminate();
        await demo.WaitForExitAsync(TimeSpan.FromSeconds(10));
        Assert.True(stopping.Elapsed < TimeSpan.FromSeconds(5), $"stop took {stopping.Elapsed}");
        await demo.DisposeAsync();

        // Restarted under another node id: the job must not depend on lease expiry or own-node recovery.
        await using var restarted = await DemoProcess.StartAsync(db, workers: 1, env: new Dictionary<string, string> { ["NODE_ID"] = "other-node" });
        var interrupted = await restarted.Api.GetJobAsync(id);
        Assert.Equal("interrupted", interrupted.AttemptHistory[0].Outcome);
        Assert.Contains(interrupted.History, h => h.Reason == "interrupted_by_shutdown");
        Assert.DoesNotContain(interrupted.History, h => h.Reason == "recovered_after_crash");
        Assert.Contains(interrupted.State, new[] { "enqueued", "processing" });
    }
}
