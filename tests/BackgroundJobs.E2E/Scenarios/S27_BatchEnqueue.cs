using System.Net;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// IJobClient.EnqueueManyAsync: many jobs in one atomic store call, each with its own options and idempotency key.
[Collection(E2ECollection.Name)]
public sealed class S27_BatchEnqueue(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_batch_creates_every_job_with_its_own_options_and_returns_ids_in_order(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var jobs = Enumerable.Range(0, 50).Select(i => (object)new { job = "log-message", @params = new { message = $"m{i}" } }).ToList();
        jobs.Add(new { job = "succeed-after", @params = new { delayMs = 0 }, options = new { runAt = DateTimeOffset.UtcNow.AddMinutes(10), maxAttempts = 7 } });
        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/jobs/batch", new { jobs }));

        Assert.Equal(51, ids.Distinct().Count());
        for (var i = 0; i < 50; i++)
        {
            var job = await demo.Api.WaitForStateAsync(ids[i], "succeeded");
            Assert.Equal($"m{i}", job.Args.GetProperty("message").GetString());
        }
        var scheduled = await demo.Api.GetJobAsync(ids[50]);
        Assert.Equal(("succeed-after", "scheduled", 7), (scheduled.Name, scheduled.State, scheduled.MaxAttempts));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task One_invalid_job_rejects_the_whole_batch(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var response = await demo.Api.PostRawAsync("/demo/jobs/batch", new
        {
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "ok" } },
                new { job = "log-message", @params = new { message = "bad queue" }, options = new { queue = "Not A Queue" } },
            },
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("jobs[1]", await response.Content.ReadAsStringAsync());
        var list = await (await demo.Api.ListRawAsync()).Content.ReadAsStringAsync();
        Assert.Empty(JsonNode.Parse(list)!["items"]!.AsArray());
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Keys_held_before_or_earlier_in_the_batch_yield_the_holder(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        var later = new { runAt = DateTimeOffset.UtcNow.AddMinutes(10) };
        var holder = await demo.Api.EnqueueAsync("log-message", new { message = "holder" }, new { idempotencyKey = "held", later.runAt });

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/jobs/batch", new
        {
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "a" }, options = new { idempotencyKey = "held" } },
                new { job = "log-message", @params = new { message = "b" }, options = new { idempotencyKey = "new", later.runAt } },
                new { job = "log-message", @params = new { message = "c" }, options = new { idempotencyKey = "new" } },
            },
        }));

        Assert.Equal(holder, ids[0]);
        Assert.NotEqual(holder, ids[1]);
        Assert.Equal(ids[1], ids[2]);
        Assert.Equal("b", (await demo.Api.GetJobAsync(ids[1])).Args.GetProperty("message").GetString());
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_batch_carries_continuations_and_retry_backoff(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        var parent = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 1500 });

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/jobs/batch", new
        {
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "after" }, options = new { continueAfter = parent } },
                new { job = "log-message", @params = new { message = "never" }, options = new { continueAfter = parent, condition = "on_finished", idempotencyKey = "c" } },
                new { job = "fail-n-times", @params = new { failTimes = 2 },
                      options = new { maxAttempts = 3, retryDelayMs = 200, retryBackoff = "exponential", retryBackoffMultiplier = 3.0 } },
            },
        }));

        Assert.Equal("awaiting", (await demo.Api.GetJobAsync(ids[0])).State);
        Assert.Equal("awaiting", (await demo.Api.GetJobAsync(ids[1])).State);
        await demo.Api.WaitForStateAsync(ids[0], "succeeded");
        await demo.Api.WaitForStateAsync(ids[1], "succeeded");

        // Retries after 200 ms, then 600 ms: the second gap is the backed-off one.
        var retried = await demo.Api.WaitForStateAsync(ids[2], "succeeded");
        var attempts = retried.AttemptHistory;
        Assert.Equal(3, attempts.Count);
        var gap = attempts[2].StartedAt - attempts[1].FinishedAt!.Value;
        Assert.True(gap >= TimeSpan.FromMilliseconds(550), $"second retry after {gap.TotalMilliseconds} ms");
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task A_continuation_of_a_missing_parent_rejects_the_whole_batch(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var response = await demo.Api.PostRawAsync("/demo/jobs/batch", new
        {
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "ok" } },
                new { job = "log-message", @params = new { message = "orphan" }, options = new { continueAfter = Guid.CreateVersion7() } },
            },
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var list = await (await demo.Api.ListRawAsync()).Content.ReadAsStringAsync();
        Assert.Empty(JsonNode.Parse(list)!["items"]!.AsArray());
    }
}
