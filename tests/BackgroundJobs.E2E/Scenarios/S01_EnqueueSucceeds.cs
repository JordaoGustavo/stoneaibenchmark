using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S01_EnqueueSucceeds(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Enqueued_job_runs_to_succeeded_with_history_and_attempt(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var id = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        var job = await demo.Api.WaitForStateAsync(id, "succeeded");

        Assert.Equal("succeed-after", job.Name);
        Assert.Equal(0, job.Args.GetProperty("delayMs").GetInt32());
        Assert.Equal(1, job.Attempts);
        Assert.Equal(3, job.MaxAttempts);
        Assert.Equal(1000, job.RetryDelayMs);
        Assert.Null(job.ScheduledAt);
        Assert.Equal(["enqueued", "processing", "succeeded"], job.History.Select(h => h.State));
        Assert.Equal(["created", "claimed", "succeeded"], job.History.Select(h => h.Reason));
        var attempt = Assert.Single(job.AttemptHistory);
        Assert.Equal(1, attempt.Attempt);
        Assert.Equal("succeeded", attempt.Outcome);
        Assert.NotNull(attempt.FinishedAt);
        Assert.Null(attempt.Error);

        var events = await demo.Api.EventsAsync();
        Assert.Equal(["started", "finished"], events.Where(e => e.JobId == id).Select(e => e.Type));
    }
}
