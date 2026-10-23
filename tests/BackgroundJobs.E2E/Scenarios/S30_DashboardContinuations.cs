using System.Net;
using System.Net.Http.Json;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// Linked jobs from the dashboard: "Run after" on the trigger form, "Add continuation" on a job page, and the chain
/// builder (/jobs/chain), all as plain form posts.
[Collection(E2ECollection.Name)]
public sealed class S30_DashboardContinuations(PostgresFixture pg)
{
    private static FormUrlEncodedContent Form(params (string Key, string Value)[] fields) =>
        new(fields.Select(f => new KeyValuePair<string, string>(f.Key, f.Value)));

    private static HttpClient NoRedirects(DemoProcess demo) =>
        new(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = demo.Api.BaseAddress };

    private static async Task<int> CountJobsAsync(DemoProcess demo)
    {
        var body = await (await demo.Api.ListRawAsync()).Content.ReadFromJsonAsync<System.Text.Json.JsonElement>();
        return body.GetProperty("items").GetArrayLength();
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Job_page_leads_to_a_prefilled_form_that_enqueues_an_awaiting_continuation(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = NoRedirects(demo);
        var parent = await demo.Api.EnqueueAsync("log-message", new { message = "first" }, new { runAt = DateTimeOffset.UtcNow.AddMinutes(10) });

        Assert.Contains($"href=\"/jobs/new?after={parent}\"", await http.GetStringAsync($"/jobs/{parent}"));
        var picker = await http.GetStringAsync($"/jobs/new?after={parent}");
        Assert.Contains("Add a continuation", picker);
        Assert.Contains($"href=\"/jobs/new/log-message?after={parent}\"", picker);
        Assert.Contains($"href=\"/jobs/chain?after={parent}\"", picker);

        var form = await http.GetStringAsync($"/jobs/new/log-message?after={parent}");
        Assert.Contains($"name=\"after.id\" value=\"{parent}\"", form);
        Assert.Contains("waits in <b>awaiting</b>", form);
        Assert.Contains("value=\"on_finished\"", form);

        var posted = await http.PostAsync("/jobs/new/log-message", Form(("arg.message", "second"), ("after.id", parent), ("after.condition", "on_finished")));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var child = await demo.Api.GetJobAsync(posted.Headers.Location!.OriginalString.Split('/', '?')[2]);
        Assert.Equal(("awaiting", parent, "on_finished"), (child.State, child.ContinueAfter, child.ContinuationCondition));
        Assert.Contains($"/jobs/{child.Id}", await http.GetStringAsync($"/jobs/{parent}"));
    }

    [Fact]
    public async Task Run_after_rejects_bad_ids_and_explains_a_finished_parent()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        using var http = NoRedirects(demo);

        var notGuid = await http.PostAsync("/jobs/new/log-message", Form(("arg.message", "x"), ("after.id", "nope")));
        Assert.Equal(HttpStatusCode.BadRequest, notGuid.StatusCode);
        Assert.Contains("is not a job id", await notGuid.Content.ReadAsStringAsync());

        var missing = Guid.CreateVersion7();
        var unknown = await http.PostAsync("/jobs/new/log-message", Form(("arg.message", "x"), ("after.id", missing.ToString())));
        Assert.Equal(HttpStatusCode.BadRequest, unknown.StatusCode);
        Assert.Contains($"no job with id {missing} exists", await unknown.Content.ReadAsStringAsync());
        Assert.Equal(0, await CountJobsAsync(demo));

        var done = await demo.Api.EnqueueAsync("log-message", new { message = "done" });
        await demo.Api.WaitForStateAsync(done, "succeeded");
        Assert.Contains("already finished", await http.GetStringAsync($"/jobs/new/log-message?after={done}"));
        Assert.Contains("decided at once", await http.GetStringAsync($"/jobs/{done}"));
        var posted = await http.PostAsync("/jobs/new/log-message", Form(("arg.message", "late"), ("after.id", done)));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        await demo.Api.WaitForStateAsync(posted.Headers.Location!.OriginalString.Split('/', '?')[2], "succeeded");
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Chain_builder_enqueues_every_step_linked_to_the_previous(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = NoRedirects(demo);

        // Editing posts re-render the chain with the values typed so far.
        var added = await (await http.PostAsync("/jobs/chain", Form(
            ("steps", "1"), ("s0.job", "succeed-after"), ("s0.arg.delayMs", "2000"), ("add.job", "log-message"), ("op", "add")))).Content.ReadAsStringAsync();
        Assert.Contains("name=\"s0.arg.delayMs\" value=\"2000\"", added);
        Assert.Contains("name=\"s1.job\" value=\"log-message\"", added);
        Assert.Contains("name=\"s1.cond\" value=\"on_finished\"", added);

        var moved = await (await http.PostAsync("/jobs/chain", Form(
            ("steps", "2"), ("s0.job", "succeed-after"), ("s0.arg.delayMs", "2000"), ("s1.job", "log-message"), ("s1.arg.message", "m"), ("op", "up:1")))).Content.ReadAsStringAsync();
        Assert.Contains("name=\"s0.arg.message\" value=\"m\"", moved);
        Assert.Contains("name=\"s1.arg.delayMs\" value=\"2000\"", moved);

        var posted = await http.PostAsync("/jobs/chain", Form(
            ("steps", "3"),
            ("s0.job", "succeed-after"), ("s0.arg.delayMs", "2000"),
            ("s1.job", "log-message"), ("s1.arg.message", "b"), ("s1.cond", "on_succeeded"),
            ("s2.job", "log-message"), ("s2.arg.message", "c"), ("s2.cond", "on_finished"),
            ("op", "enqueue")));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var location = posted.Headers.Location!.OriginalString;
        Assert.StartsWith("/jobs/chain/result?ids=", location);
        var ids = location.Split('=')[1].Split(',');
        Assert.Equal(3, ids.Length);

        var a = await demo.Api.GetJobAsync(ids[0]);
        var b = await demo.Api.GetJobAsync(ids[1]);
        var c = await demo.Api.GetJobAsync(ids[2]);
        Assert.Null(a.ContinueAfter);
        Assert.Contains(a.State, new[] { "enqueued", "processing" });
        Assert.Equal(("awaiting", ids[0], "on_succeeded"), (b.State, b.ContinueAfter, b.ContinuationCondition));
        Assert.Equal(("awaiting", ids[1], "on_finished"), (c.State, c.ContinueAfter, c.ContinuationCondition));

        var result = await http.GetStringAsync(location);
        Assert.Contains("Chain enqueued", result);
        foreach (var id in ids) Assert.Contains($"href=\"/jobs/{id}\"", result);

        await demo.Api.WaitForStateAsync(ids[2], "succeeded");
    }

    [Fact]
    public async Task An_invalid_step_enqueues_nothing_and_a_chain_can_start_after_an_existing_job()
    {
        await using var demo = await DemoProcess.StartAsync(null);
        using var http = NoRedirects(demo);

        var invalid = await http.PostAsync("/jobs/chain", Form(
            ("steps", "2"), ("s0.job", "log-message"), ("s0.arg.message", "ok"),
            ("s1.job", "succeed-after"), ("s1.arg.delayMs", "soon"), ("s1.cond", "on_succeeded"), ("op", "enqueue")));
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
        var page = await invalid.Content.ReadAsStringAsync();
        Assert.Contains("Step 2 (succeed-after): Delay ms: a whole number expected.", page);
        Assert.Contains("name=\"s0.arg.message\" value=\"ok\"", page);
        Assert.Equal(0, await CountJobsAsync(demo));

        var anchor = await demo.Api.EnqueueAsync("log-message", new { message = "anchor" }, new { runAt = DateTimeOffset.UtcNow.AddMinutes(10) });
        Assert.Contains($"name=\"after.id\" value=\"{anchor}\"", await http.GetStringAsync($"/jobs/chain?after={anchor}"));
        var posted = await http.PostAsync("/jobs/chain", Form(
            ("after.id", anchor), ("after.condition", "on_finished"),
            ("steps", "1"), ("s0.job", "log-message"), ("s0.arg.message", "next"), ("op", "enqueue")));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var child = await demo.Api.GetJobAsync(posted.Headers.Location!.OriginalString.Split('=')[1]);
        Assert.Equal(("awaiting", anchor, "on_finished"), (child.State, child.ContinueAfter, child.ContinuationCondition));
    }
}
