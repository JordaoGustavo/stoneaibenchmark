using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S19_IdempotencyKeys(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Enqueueing_a_key_held_by_an_unfinished_job_returns_that_job(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var first = await demo.Api.EnqueueAsync("log-message", new { message = "once" },
            new { idempotencyKey = "order-42", runAt = DateTimeOffset.UtcNow.AddMinutes(10) });
        var second = await demo.Api.EnqueueAsync("log-message", new { message = "twice" }, new { idempotencyKey = "order-42" });
        var other = await demo.Api.EnqueueAsync("log-message", new { message = "other" }, new { idempotencyKey = "order-43" });

        Assert.Equal(first, second);
        Assert.NotEqual(first, other);
        var job = await demo.Api.GetJobAsync(first);
        Assert.Equal(("scheduled", "order-42"), (job.State, job.IdempotencyKey));
        Assert.Equal("once", job.Args.GetProperty("message").GetString());
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Concurrent_enqueues_with_one_key_create_one_job(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var ids = await Task.WhenAll(Enumerable.Range(0, 20).Select(i =>
            demo.Api.EnqueueAsync("succeed-after", new { delayMs = 500 }, new { idempotencyKey = "burst" })));

        Assert.Single(ids.Distinct());
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Once_the_job_finished_its_key_is_free_again(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var first = await demo.Api.EnqueueAsync("log-message", new { message = "a" }, new { idempotencyKey = "k" });
        await demo.Api.WaitForStateAsync(first, "succeeded");
        var second = await demo.Api.EnqueueAsync("log-message", new { message = "b" }, new { idempotencyKey = "k" });

        Assert.NotEqual(first, second);
        await demo.Api.WaitForStateAsync(second, "succeeded");
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Requeueing_a_finished_job_whose_key_is_taken_again_conflicts(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var first = await demo.Api.EnqueueAsync("log-message", new { message = "a" }, new { idempotencyKey = "k" });
        await demo.Api.WaitForStateAsync(first, "succeeded");
        var second = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 },
            new { idempotencyKey = "k", runAt = DateTimeOffset.UtcNow.AddMinutes(10) });

        var response = await demo.Api.RequeueAsync(first);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains(second, await response.Content.ReadAsStringAsync());
        Assert.Equal("succeeded", (await demo.Api.GetJobAsync(first)).State);

        // Freed once the holder is gone.
        await demo.Api.DeleteAsync(second);
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.RequeueAsync(first)).StatusCode);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Blank_or_overlong_keys_are_rejected(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        foreach (var key in new[] { " ", new string('x', 201) })
        {
            var response = await demo.Api.PostJobRawAsync(new { job = "log-message", @params = new { message = "m" }, options = new { idempotencyKey = key } });
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
    }
}
