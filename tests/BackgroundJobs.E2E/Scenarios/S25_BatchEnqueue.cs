using System.Net;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// IJobClient.EnqueueManyAsync: many jobs in one atomic store call, each with its own options and idempotency key.
[Collection(E2ECollection.Name)]
public sealed class S25_BatchEnqueue(PostgresFixture pg)
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
}
