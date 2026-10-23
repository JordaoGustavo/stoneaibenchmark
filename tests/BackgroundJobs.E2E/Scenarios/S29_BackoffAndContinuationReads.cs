using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// Retry backoff settings and continuations are readable back: on the job view, through GET /api/jobs/{id}/children and
/// on the dashboard, which can also enqueue with backoff options.
[Collection(E2ECollection.Name)]
public sealed class S29_BackoffAndContinuationReads(PostgresFixture pg)
{
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Job_view_shows_the_retry_settings_it_was_enqueued_with(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        var later = new { runAt = DateTimeOffset.UtcNow.AddMinutes(5) };

        var backedOff = await demo.Api.GetJobAsync(await demo.Api.EnqueueAsync("log-message", new { message = "x" }, new
        {
            later.runAt, retryDelayMs = 200, retryJitterMs = 50, retryBackoff = "exponential", retryBackoffMultiplier = 2.5, maxRetryDelayMs = 4000,
        }));
        Assert.Equal((200, 50, "exponential", 2.5, (int?)4000),
            (backedOff.RetryDelayMs, backedOff.RetryJitterMs, backedOff.RetryBackoff, backedOff.RetryBackoffMultiplier, backedOff.MaxRetryDelayMs));

        var plain = await demo.Api.GetJobAsync(await demo.Api.EnqueueAsync("log-message", new { message = "y" }, later));
        Assert.Equal(("fixed", 1d, (int?)null), (plain.RetryBackoff, plain.RetryBackoffMultiplier, plain.MaxRetryDelayMs));
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Children_endpoint_lists_a_jobs_continuations_in_every_state(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));

        var parent = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 }, new { runAt = DateTimeOffset.UtcNow.AddMinutes(5) });
        var children = new List<string>();
        for (var i = 0; i < 3; i++)
            children.Add(await demo.Api.EnqueueAsync("log-message", new { message = $"c{i}" }, new { continueAfter = parent }));
        var unrelated = await demo.Api.EnqueueAsync("log-message", new { message = "other" });

        var page = await (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children")).Content.ReadFromJsonAsync<JsonObject>();
        var items = page!["items"]!.AsArray();
        Assert.Equal(children, items.Select(i => i!["id"]!.GetValue<string>()));
        Assert.All(items, i => Assert.Equal((parent, "awaiting"), (i!["continueAfter"]!.GetValue<string>(), i["state"]!.GetValue<string>())));
        Assert.Null(page["nextCursor"]);

        var first = await (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children?limit=2")).Content.ReadFromJsonAsync<JsonObject>();
        var cursor = first!["nextCursor"]!.GetValue<string>();
        var second = await (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children?after={cursor}&limit=2")).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Equal(children[2..], second!["items"]!.AsArray().Select(i => i!["id"]!.GetValue<string>()));

        // Released children are still listed; a job without continuations has none.
        Assert.Equal(HttpStatusCode.Accepted, (await demo.Api.CancelAsync(parent)).StatusCode);
        foreach (var c in children) await demo.Api.WaitForStateAsync(c, "cancelled");
        var after = await (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children")).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Equal(children, after!["items"]!.AsArray().Select(i => i!["id"]!.GetValue<string>()));
        var none = await (await demo.Api.GetRawAsync($"/api/jobs/{unrelated}/children")).Content.ReadFromJsonAsync<JsonObject>();
        Assert.Empty(none!["items"]!.AsArray());

        Assert.Equal(HttpStatusCode.NotFound, (await demo.Api.GetRawAsync("/api/jobs/not-an-id/children")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children?limit=0")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await demo.Api.GetRawAsync($"/api/jobs/{parent}/children?after=nope")).StatusCode);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Dashboard_shows_backoff_and_links_parent_and_children(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };

        var parent = await demo.Api.EnqueueAsync("log-message", new { message = "p" }, new
        {
            runAt = DateTimeOffset.UtcNow.AddMinutes(5), retryJitterMs = 300, retryBackoff = "exponential", retryBackoffMultiplier = 3, maxRetryDelayMs = 60000,
        });
        var child = await demo.Api.EnqueueAsync("log-message", new { message = "c" }, new { continueAfter = parent });

        var parentPage = await http.GetStringAsync($"/jobs/{parent}");
        Assert.Contains("<dt>Retry jitter</dt><dd>300 ms</dd>", parentPage);
        Assert.Contains("<dt>Backoff</dt><dd>exponential &times;3, max 1 min</dd>", parentPage);
        Assert.Contains("<h2>Continuations</h2>", parentPage);
        Assert.Contains($"href=\"/jobs/{child}\"", parentPage);

        var childPage = await http.GetStringAsync($"/jobs/{child}");
        Assert.Contains("<dt>Backoff</dt><dd>fixed</dd>", childPage);
        Assert.DoesNotContain("<h2>Continuations</h2>", childPage);

        // The list links a continuation to its parent.
        Assert.Contains($"after <a class=\"mono\" href=\"/jobs/{parent}\">{parent[..8]}</a>", await http.GetStringAsync("/jobs"));
    }

    [Fact]
    public async Task Dashboard_enqueue_form_sets_retry_and_backoff_options()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = demo.Api.BaseAddress };

        var form = await http.GetStringAsync("/jobs/new/log-message");
        foreach (var field in new[] { "opt.retryJitterMs", "opt.retryBackoff", "opt.retryBackoffMultiplier", "opt.maxRetryDelayMs" })
            Assert.Contains($"name=\"{field}\"", form);

        var posted = await http.PostAsync("/jobs/new/log-message", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["arg.message"] = "later",
            ["opt.runAt"] = DateTime.UtcNow.AddHours(30).ToString("yyyy-MM-ddTHH:mm"),
            ["opt.retryDelayMs"] = "100",
            ["opt.retryJitterMs"] = "20",
            ["opt.retryBackoff"] = "exponential",
            ["opt.retryBackoffMultiplier"] = "1.5",
            ["opt.maxRetryDelayMs"] = "900",
        }));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var job = await demo.Api.GetJobAsync(posted.Headers.Location!.OriginalString.Split('/', '?')[2]);
        Assert.Equal((100, 20, "exponential", 1.5, (int?)900),
            (job.RetryDelayMs, job.RetryJitterMs, job.RetryBackoff, job.RetryBackoffMultiplier, job.MaxRetryDelayMs));

        var invalid = await http.PostAsync("/jobs/new/log-message", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["arg.message"] = "x",
            ["opt.retryBackoffMultiplier"] = "0.5",
            ["opt.retryJitterMs"] = "-1",
        }));
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
        var page = await invalid.Content.ReadAsStringAsync();
        Assert.Contains("Backoff multiplier must be a number from 1 to 100.", page);
        Assert.Contains("Retry jitter must be a whole number of milliseconds, 0 or more.", page);
    }
}
