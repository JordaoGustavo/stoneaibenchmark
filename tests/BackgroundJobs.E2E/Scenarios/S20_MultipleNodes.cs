using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S20_MultipleNodes(PostgresFixture pg)
{
    [Fact]
    public async Task Starting_a_second_node_on_the_same_machine_does_not_interrupt_the_first_nodes_jobs()
    {
        var db = await pg.CreateDatabaseAsync();
        await using var first = await DemoProcess.StartAsync(db);

        var id = await first.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 });
        await first.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        // Same machine name, so a machine-derived node id would match and startup recovery would steal the job.
        await using var second = await DemoProcess.StartAsync(db);

        var job = await first.Api.WaitForStateAsync(id, "succeeded");
        Assert.Equal("succeeded", Assert.Single(job.AttemptHistory).Outcome);
    }

    [Fact]
    public async Task Jobs_of_a_dead_node_are_recovered_by_a_live_node_once_their_lease_expires()
    {
        var db = await pg.CreateDatabaseAsync();
        var shortLease = new Dictionary<string, string> { ["LEASE_MS"] = "1500" };
        var dying = await DemoProcess.StartAsync(db, env: shortLease);

        var id = await dying.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 }, new { maxAttempts = 2 });
        await dying.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "started"));
        await using var survivor = await DemoProcess.StartAsync(db, env: shortLease);
        await dying.KillAsync();

        var job = await survivor.Api.WaitForStateAsync(id, "succeeded", TimeSpan.FromSeconds(20));
        Assert.Equal(["interrupted", "succeeded"], job.AttemptHistory.Select(a => a.Outcome));
    }
}
