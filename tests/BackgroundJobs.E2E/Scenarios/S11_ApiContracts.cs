using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S11_ApiContracts(PostgresFixture pg)
{
    public static TheoryData<string> InvalidBodies => new()
    {
        """{"job":"nope","params":{}}""",
        """{"job":"succeed-after","params":{}}""",
        """{"job":"succeed-after","params":{"delayMs":-1}}""",
        """{"job":"succeed-after","params":{"delayMs":60001}}""",
        """{"job":"succeed-after","params":{"delayMs":"5"}}""",
        """{"job":"succeed-after","params":{"delayMs":1.5}}""",
        """{"job":"succeed-after","params":{"delayMs":0,"extra":1}}""",
        """{"job":"succeed-after","params":{"delayMs":0},"extra":1}""",
        """{"job":"succeed-after","params":{"delayMs":0},"options":{"maxAttempts":0}}""",
        """{"job":"succeed-after","params":{"delayMs":0},"options":{"maxAttempts":11}}""",
        """{"job":"succeed-after","params":{"delayMs":0},"options":{"retryDelayMs":60001}}""",
        """{"job":"succeed-after","params":{"delayMs":0},"options":{"runAt":"yesterday"}}""",
        """{"job":"succeed-after","params":{"delayMs":0},"options":{"priority":1}}""",
        """{"job":"fail-n-times","params":{"failTimes":11}}""",
        """{"job":"log-message","params":{"message":null}}""",
        $$$"""{"job":"log-message","params":{"message":"{{{new string('x', 201)}}}"}}""",
        """{"params":{"delayMs":0}}""",
        """not json""",
    };

    [Theory]
    [MemberData(nameof(InvalidBodies))]
    public async Task Invalid_create_requests_return_400_with_error(string body)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };

        var res = await http.PostAsync("/demo/jobs", new StringContent(body, System.Text.Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, res.StatusCode);
        var error = await res.Content.ReadFromJsonAsync<JsonObject>();
        Assert.False(string.IsNullOrEmpty(error!["error"]!.GetValue<string>()));
    }

    [Fact]
    public async Task List_returns_summaries_in_creation_order_with_cursor_pagination()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        var ids = new List<string>();
        for (var i = 0; i < 3; i++)
            ids.Add(await demo.Api.EnqueueAsync("log-message", new { message = $"m{i}" }, new { runAt = DateTime.UtcNow.AddMinutes(5) }));

        var all = await (await demo.Api.ListRawAsync()).Content.ReadFromJsonAsync<JsonObject>();
        var items = all!["items"]!.AsArray();
        Assert.Equal(ids, items.Select(i => i!["id"]!.GetValue<string>()));
        Assert.Equal(["id", "name", "state", "attempts"], items[0]!.AsObject().Select(p => p.Key));
        Assert.Equal("scheduled", items[0]!["state"]!.GetValue<string>());

        var page1 = await (await demo.Api.ListRawAsync("?limit=2")).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Equal(ids[..2], page1!["items"]!.AsArray().Select(i => i!["id"]!.GetValue<string>()));
        var cursor = page1["nextCursor"]!.GetValue<string>();
        var page2 = await (await demo.Api.ListRawAsync($"?after={cursor}&limit=2")).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Equal(ids[2..], page2!["items"]!.AsArray().Select(i => i!["id"]!.GetValue<string>()));
        Assert.Null(page2["nextCursor"]);
    }

    [Fact]
    public async Task Health_is_ok_when_ready_and_job_view_has_the_full_contract()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        var health = await demo.Api.GetHealthAsync();
        Assert.Equal("""{"status":"ok"}""", await health.Content.ReadAsStringAsync());

        var id = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        await demo.Api.WaitForStateAsync(id, "succeeded");
        var job = await (await demo.Api.GetJobRawAsync(id)).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Equal(
            ["id", "name", "args", "state", "attempts", "maxAttempts", "retryDelayMs", "scheduledAt", "createdAt", "updatedAt", "history", "attemptHistory"],
            job!.Select(p => p.Key));
        Assert.EndsWith("Z", job!["createdAt"]!.GetValue<string>());
        Assert.Equal(["state", "at", "reason"], job!["history"]![0]!.AsObject().Select(p => p.Key));
        Assert.Equal(["attempt", "startedAt", "finishedAt", "outcome", "error"], job!["attemptHistory"]![0]!.AsObject().Select(p => p.Key));
    }
}
