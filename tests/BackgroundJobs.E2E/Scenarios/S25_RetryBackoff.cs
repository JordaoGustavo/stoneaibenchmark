using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// Opt-in exponential retry backoff: per job, or as the server default. Without it retries keep the fixed delay (S05).
[Collection(E2ECollection.Name)]
public sealed class S25_RetryBackoff(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Per_job_exponential_backoff_grows_the_delay_until_the_cap(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var id = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 3 },
            new { maxAttempts = 4, retryDelayMs = 300, retryBackoff = "exponential", retryBackoffMultiplier = 2.0, maxRetryDelayMs = 700 });
        var gaps = await GapsAsync(demo, id);

        // 300 ms, 600 ms, then 1200 ms capped at 700 ms.
        Assert.True(gaps[0] >= TimeSpan.FromMilliseconds(300), $"gaps: {string.Join(", ", gaps)}");
        Assert.True(gaps[1] >= TimeSpan.FromMilliseconds(600), $"gaps: {string.Join(", ", gaps)}");
        Assert.True(gaps[2] >= TimeSpan.FromMilliseconds(700), $"gaps: {string.Join(", ", gaps)}");
        Assert.True(gaps[2] < TimeSpan.FromMilliseconds(1200), $"the cap was not applied; gaps: {string.Join(", ", gaps)}");
    }

    [Fact]
    public async Task Server_default_backoff_applies_to_jobs_that_do_not_set_one_and_jobs_can_opt_back_into_fixed()
    {
        var env = new Dictionary<string, string> { ["RETRY_BACKOFF"] = "exponential", ["RETRY_BACKOFF_MULTIPLIER"] = "3" };
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(Stores.Postgres), env: env);

        var exponential = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 2 }, new { maxAttempts = 3, retryDelayMs = 400 });
        var fixedDelay = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 2 },
            new { maxAttempts = 3, retryDelayMs = 400, retryBackoff = "fixed" });

        var gaps = await GapsAsync(demo, exponential);
        Assert.True(gaps[1] >= TimeSpan.FromMilliseconds(1200), $"gaps: {string.Join(", ", gaps)}");

        var fixedGaps = await GapsAsync(demo, fixedDelay);
        Assert.All(fixedGaps, g => Assert.True(g >= TimeSpan.FromMilliseconds(400), $"gaps: {string.Join(", ", fixedGaps)}"));
        Assert.True(fixedGaps[1] < TimeSpan.FromMilliseconds(1200), $"fixed delay grew; gaps: {string.Join(", ", fixedGaps)}");
    }

    [Fact]
    public async Task Invalid_backoff_options_are_rejected_at_enqueue()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(Stores.Memory));

        foreach (var options in new object[]
        {
            new { retryBackoff = "exponential", retryBackoffMultiplier = 0.5 },
            new { retryBackoff = "exponential", maxRetryDelayMs = -5 },
            new { retryBackoff = "linear" },
        })
        {
            var res = await demo.Api.PostJobRawAsync(new { job = "fail-n-times", @params = new { failTimes = 0 }, options });
            Assert.True(res.StatusCode == HttpStatusCode.BadRequest, $"{options}: {res.StatusCode} {await res.Content.ReadAsStringAsync()}");
        }
    }

    [Fact]
    public async Task An_invalid_server_default_stops_startup()
    {
        var env = new Dictionary<string, string> { ["RETRY_BACKOFF_MULTIPLIER"] = "0.5" };
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DemoProcess.StartAsync(connectionString: null, env: env));
        Assert.Contains("DefaultRetryBackoffMultiplier", ex.Message);
    }

    private static async Task<List<TimeSpan>> GapsAsync(DemoProcess demo, string id)
    {
        var job = await demo.Api.WaitForStateAsync(id, "succeeded");
        return Enumerable.Range(1, job.AttemptHistory.Count - 1)
            .Select(i => job.AttemptHistory[i].StartedAt - job.AttemptHistory[i - 1].FinishedAt!.Value)
            .ToList();
    }
}
