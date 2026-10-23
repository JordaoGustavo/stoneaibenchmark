using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// The dashboard ships in its own package (BackgroundJobs.Dashboard) and only uses the core's public API.
[Collection(E2ECollection.Name)]
public sealed class S13_Dashboard(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Dashboard_package_renders_jobs(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        var id = await demo.Api.EnqueueAsync("log-message", new { message = "hi" });
        await demo.Api.WaitForStateAsync(id, "succeeded");

        var list = await demo.Api.GetRawAsync("/jobs");
        Assert.Equal(HttpStatusCode.OK, list.StatusCode);
        Assert.Contains("log-message", await list.Content.ReadAsStringAsync());
        var detail = await demo.Api.GetRawAsync($"/jobs/{id}");
        Assert.Equal(HttpStatusCode.OK, detail.StatusCode);
        Assert.Contains(id, await detail.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Dashboard_cancels_a_waiting_job()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        var id = await demo.Api.EnqueueAsync("log-message", new { message = "never" }, new { runAt = DateTime.UtcNow.AddMinutes(5) });

        Assert.Contains($"action=\"/jobs/{id}/cancel\"", await (await demo.Api.GetRawAsync($"/jobs/{id}")).Content.ReadAsStringAsync());
        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };
        await http.PostAsync($"/jobs/{id}/cancel", null);

        Assert.Equal("cancelled", (await demo.Api.GetJobAsync(id)).State);
        Assert.DoesNotContain($"action=\"/jobs/{id}/cancel\"", await (await demo.Api.GetRawAsync($"/jobs/{id}")).Content.ReadAsStringAsync());
    }
}
