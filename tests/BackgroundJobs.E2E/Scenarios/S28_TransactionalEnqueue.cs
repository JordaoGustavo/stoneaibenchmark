using System.Net;
using BackgroundJobs.E2E.Infra;
using Npgsql;

namespace BackgroundJobs.E2E.Scenarios;

/// Outbox pattern: the demo's /demo/orders writes an order row and enqueues its jobs on one NpgsqlTransaction
/// (IJobClient.EnqueueManyAsync(transaction, ...)). The jobs exist only if the order commits. Postgres only.
[Collection(E2ECollection.Name)]
public sealed class S28_TransactionalEnqueue(PostgresFixture pg)
{
    [Fact]
    public async Task Committed_jobs_run_with_the_order_and_the_commit_wakes_the_scheduler()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);
        // The scheduler sleeps up to SchedulerMaxWait (5 s) without a notification; only the NOTIFY delivered on commit
        // makes it notice the scheduled job in time.
        var runAt = DateTimeOffset.UtcNow.AddSeconds(1.5);

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-1",
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "now" } },
                new { job = "log-message", @params = new { message = "later" }, options = new { runAt } },
            },
        }));

        Assert.Equal(1, await CountOrdersAsync(db, "o-1"));
        await demo.Api.WaitForStateAsync(ids[0], "succeeded");
        var later = await demo.Api.WaitForStateAsync(ids[1], "succeeded", TimeSpan.FromSeconds(10));
        var promoted = later.History.First(h => h.State == "enqueued").At;
        Assert.True(promoted < runAt.UtcDateTime.AddSeconds(2), $"promoted at {promoted:O}, run_at {runAt:O}");
    }

    [Fact]
    public async Task Rolled_back_jobs_never_exist_and_free_their_key()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);

        var rolledBack = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-1",
            rollback = true,
            jobs = new object[] { new { job = "log-message", @params = new { message = "x" }, options = new { idempotencyKey = "order:o-1" } } },
        }));

        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetJobRawAsync(rolledBack[0])).StatusCode);
        Assert.Equal(0, await CountOrdersAsync(db, "o-1"));

        var committed = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-1",
            jobs = new object[] { new { job = "log-message", @params = new { message = "x" }, options = new { idempotencyKey = "order:o-1" } } },
        }));
        Assert.NotEqual(rolledBack[0], committed[0]);
        await demo.Api.WaitForStateAsync(committed[0], "succeeded");
    }

    [Fact]
    public async Task A_key_enqueued_earlier_on_the_transaction_or_held_outside_it_yields_the_holder()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);
        var later = DateTimeOffset.UtcNow.AddMinutes(10);
        var outside = await demo.Api.EnqueueAsync("log-message", new { message = "outside" }, new { idempotencyKey = "held", runAt = later });

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-2",
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "a" }, options = new { idempotencyKey = "k", runAt = later } },
                new { job = "log-message", @params = new { message = "b" }, options = new { idempotencyKey = "k" } },
                new { job = "log-message", @params = new { message = "c" }, options = new { idempotencyKey = "held" } },
            },
        }));

        Assert.Equal(ids[0], ids[1]);
        Assert.Equal(outside, ids[2]);
        Assert.Equal("scheduled", (await demo.Api.GetJobAsync(ids[0])).State);
    }

    [Fact]
    public async Task An_invalid_job_fails_before_the_transaction_writes_anything()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);

        var response = await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-3",
            jobs = new object[] { new { job = "log-message", @params = new { message = "x" }, options = new { priority = 1_000_000 } } },
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await CountOrdersAsync(db, "o-3"));
    }

    [Fact]
    public async Task A_parent_and_its_continuation_enqueued_on_one_transaction_run_in_order_after_commit()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-4",
            chain = true,
            jobs = new object[]
            {
                new { job = "succeed-after", @params = new { delayMs = 500 } },
                new { job = "log-message", @params = new { message = "after" } },
            },
        }));

        Assert.Equal(ids[0], (await demo.Api.GetJobAsync(ids[1])).ContinueAfter);
        var child = await demo.Api.WaitForStateAsync(ids[1], "succeeded");
        Assert.Equal(["awaiting", "enqueued", "processing", "succeeded"], child.History.Select(h => h.State));
        var parent = await demo.Api.GetJobAsync(ids[0]);
        Assert.True(child.History[1].At >= parent.History.Last().At);
    }

    [Fact]
    public async Task A_rolled_back_chain_leaves_nothing()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var demo = await DemoProcess.StartAsync(db);

        var ids = await ApiClient.JobIdsAsync(await demo.Api.PostRawAsync("/demo/orders", new
        {
            orderId = "o-5",
            chain = true,
            rollback = true,
            jobs = new object[]
            {
                new { job = "log-message", @params = new { message = "parent" } },
                new { job = "log-message", @params = new { message = "child" } },
            },
        }));

        foreach (var id in ids) Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetJobRawAsync(id)).StatusCode);
    }

    private static async Task<long> CountOrdersAsync(string connectionString, string id)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS demo_orders (id text PRIMARY KEY); SELECT count(*) FROM demo_orders WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
