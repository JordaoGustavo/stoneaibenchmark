using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S06_ExhaustedRetries(PostgresFixture pg)
{
    [Fact]
    public async Task Job_fails_after_max_attempts_while_others_keep_running()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());

        var failing = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 10 }, new { maxAttempts = 3, retryDelayMs = 200 });
        var ok = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });

        var failed = await demo.Api.WaitForStateAsync(failing, "failed");
        Assert.Equal(3, failed.Attempts);
        Assert.Equal(["failed", "failed", "failed"], failed.AttemptHistory.Select(a => a.Outcome));
        Assert.Equal("max_attempts_exhausted", failed.History[^1].Reason);

        Assert.Equal(1, (await demo.Api.WaitForStateAsync(ok, "succeeded")).Attempts);
    }
}
