using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S03_FifoSingleWorker(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Messages_run_in_enqueue_order_with_one_worker(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store), workers: 1);

        foreach (var message in new[] { "A", "B", "C" })
            await demo.Api.EnqueueAsync("log-message", new { message });

        var events = await demo.Api.WaitForEventsAsync(e => e.Count(x => x.Type == "message") == 3);
        Assert.Equal(["A", "B", "C"], events.Where(e => e.Type == "message").Select(e => e.Detail!.Value.GetProperty("message").GetString()));
    }
}
