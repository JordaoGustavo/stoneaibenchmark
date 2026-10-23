using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S09_CrashRecovery(PostgresFixture pg)
{
    [Theory]
    [InlineData(3, "succeeded", new[] { "interrupted", "succeeded" })]
    [InlineData(1, "failed", new[] { "interrupted" })]
    public async Task Interrupted_attempt_is_recorded_and_counts_against_max_attempts(int maxAttempts, string finalState, string[] outcomes)
    {
        var db = await pg.CreateDatabaseAsync();
        // Recovery waits for the lease to expire, even on a restart of the same machine.
        var shortLease = new Dictionary<string, string> { ["LEASE_MS"] = "1500" };
        var demo = await DemoProcess.StartAsync(db, env: shortLease);

        var id = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 }, new { maxAttempts });
        await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        await demo.KillAsync();

        await using var restarted = await DemoProcess.StartAsync(db, env: shortLease);
        var job = await restarted.Api.WaitForStateAsync(id, finalState);

        Assert.Equal(outcomes, job.AttemptHistory.Select(a => a.Outcome));
        Assert.Equal(outcomes.Length, job.Attempts);
        Assert.NotNull(job.AttemptHistory[0].FinishedAt);
        Assert.Contains(job.History, h => h.Reason == (maxAttempts > 1 ? "recovered_after_crash" : "max_attempts_exhausted"));
    }
}
