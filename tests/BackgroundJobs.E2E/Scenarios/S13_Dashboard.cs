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

    /// Newest first by default; the Created header flips to oldest first.
    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Dashboard_sorts_by_created(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        var older = await demo.Api.EnqueueAsync("log-message", new { message = "older" });
        var newer = await demo.Api.EnqueueAsync("log-message", new { message = "newer" });

        var desc = await (await demo.Api.GetRawAsync("/jobs")).Content.ReadAsStringAsync();
        Assert.True(desc.IndexOf($"/jobs/{newer}\"") < desc.IndexOf($"/jobs/{older}\""));
        Assert.Contains("href=\"/jobs?dir=asc\"", desc);

        var asc = await (await demo.Api.GetRawAsync("/jobs?dir=asc")).Content.ReadAsStringAsync();
        Assert.True(asc.IndexOf($"/jobs/{older}\"") < asc.IndexOf($"/jobs/{newer}\""));
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

    /// Same conflict as the API's 409 (S19): the dashboard shows it instead of failing with a 500.
    [Fact]
    public async Task Dashboard_requeue_whose_idempotency_key_is_taken_shows_the_conflict()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        var first = await demo.Api.EnqueueAsync("log-message", new { message = "a" }, new { idempotencyKey = "k" });
        await demo.Api.WaitForStateAsync(first, "succeeded");
        var holder = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 },
            new { idempotencyKey = "k", runAt = DateTimeOffset.UtcNow.AddMinutes(10) });

        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };
        var response = await http.PostAsync($"/jobs/{first}/requeue", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains(holder, await response.Content.ReadAsStringAsync());
        Assert.Equal("succeeded", (await demo.Api.GetJobAsync(first)).State);
    }

    /// A first visit adopts the browser's time zone via a cookie; once the cookie is set the script is gone.
    [Fact]
    public async Task Dashboard_detects_browser_time_zone_once()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };
        var first = await http.GetAsync("/jobs");
        var html = await first.Content.ReadAsStringAsync();
        Assert.Contains("Intl.DateTimeFormat().resolvedOptions().timeZone", html);
        // The CSP must let that inline script run, or the browser silently drops it.
        var nonce = System.Text.RegularExpressions.Regex.Match(first.Headers.GetValues("Content-Security-Policy").Single(), "'nonce-([^']+)'").Groups[1].Value;
        Assert.Contains($"<script nonce=\"{nonce}\">", html);

        http.DefaultRequestHeaders.Add("Cookie", "bj_tz=America/Sao_Paulo");
        Assert.DoesNotContain("<script", await http.GetStringAsync("/jobs"));
    }

    /// The Run at picker is a wall-clock time in the viewer's chosen zone, not UTC.
    [Fact]
    public async Task Dashboard_run_at_follows_viewer_time_zone()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        var zone = TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo");
        var wallClock = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow.AddHours(2), zone);
        wallClock = new DateTime(wallClock.Year, wallClock.Month, wallClock.Day, wallClock.Hour, wallClock.Minute, 0);

        using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = demo.Api.BaseAddress };
        http.DefaultRequestHeaders.Add("Cookie", "bj_tz=America/Sao_Paulo");
        Assert.Contains("blank runs now · America/Sao_Paulo", await http.GetStringAsync("/jobs/new/log-message"));

        var posted = await http.PostAsync("/jobs/new/log-message", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["arg.message"] = "later",
            ["opt.runAt"] = wallClock.ToString("yyyy-MM-ddTHH:mm"),
        }));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var id = posted.Headers.Location!.OriginalString.Split('/', '?')[2];

        var job = await demo.Api.GetJobAsync(id);
        Assert.Equal("scheduled", job.State);
        Assert.Equal(TimeZoneInfo.ConvertTimeToUtc(wallClock, zone), job.ScheduledAt!.Value.ToUniversalTime());
    }
}
