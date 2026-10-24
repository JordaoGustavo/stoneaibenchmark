using System.Net;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

/// "Add step" on the single-job form turns it into a chain whose step 1 keeps everything typed, and every chain step
/// carries the full set of per-job options.
[Collection(E2ECollection.Name)]
public sealed class S31_DashboardChainFromSingleForm(PostgresFixture pg)
{
    private static FormUrlEncodedContent Form(params (string Key, string Value)[] fields) =>
        new(fields.Select(f => new KeyValuePair<string, string>(f.Key, f.Value)));

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Single_form_becomes_a_chain_and_step_options_reach_the_jobs(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = demo.Api.BaseAddress };
        var anchor = await demo.Api.EnqueueAsync("log-message", new { message = "anchor" }, new { runAt = DateTimeOffset.UtcNow.AddMinutes(10) });

        // The trigger form shows every option, server defaults, registration settings and the "Add step" row.
        var single = await http.GetStringAsync("/jobs/new/log-message");
        foreach (var field in new[] { "runAt", "maxAttempts", "retryDelayMs", "retryJitterMs", "retryBackoff", "retryBackoffMultiplier", "maxRetryDelayMs", "queue", "priority", "idempotencyKey" })
            Assert.Contains($"name=\"opt.{field}\"", single);
        Assert.Contains("placeholder=\"3\"", single);
        Assert.Contains("Fixed for this job type", single);
        Assert.Contains("formaction=\"/jobs/chain\"", single);

        // Add step from the single form: step 1 keeps args, options and the run-after parent.
        var chain = await http.PostAsync("/jobs/chain", Form(
            ("single", "log-message"), ("arg.message", "one"), ("opt.priority", "7"), ("opt.queue", "default"), ("opt.maxAttempts", "4"),
            ("after.id", anchor), ("after.condition", "on_finished"), ("add.job", "succeed-after"), ("op", "add")));
        Assert.Equal(HttpStatusCode.OK, chain.StatusCode);
        var page = await chain.Content.ReadAsStringAsync();
        Assert.Contains("name=\"s0.job\" value=\"log-message\"", page);
        Assert.Contains("name=\"s0.arg.message\" value=\"one\"", page);
        Assert.Contains("name=\"s0.opt.priority\" value=\"7\"", page);
        Assert.Contains("name=\"s0.opt.maxAttempts\" value=\"4\"", page);
        Assert.Contains($"name=\"after.id\" value=\"{anchor}\"", page);
        Assert.Contains("name=\"after.condition\" value=\"on_finished\" checked", page);
        Assert.Contains("name=\"s1.job\" value=\"succeed-after\"", page);
        Assert.Contains("name=\"s1.opt.idempotencyKey\"", page);

        // Options follow their step when it moves.
        var moved = await (await http.PostAsync("/jobs/chain", Form(
            ("steps", "2"), ("s0.job", "log-message"), ("s0.arg.message", "one"), ("s0.opt.priority", "7"),
            ("s1.job", "succeed-after"), ("s1.arg.delayMs", "0"), ("s1.opt.idempotencyKey", "k1"), ("op", "down:0")))).Content.ReadAsStringAsync();
        Assert.Contains("name=\"s1.opt.priority\" value=\"7\"", moved);
        Assert.Contains("name=\"s0.opt.idempotencyKey\" value=\"k1\"", moved);

        // Invalid option: per-field error, nothing created.
        var invalid = await http.PostAsync("/jobs/chain", Form(
            ("steps", "1"), ("s0.job", "log-message"), ("s0.arg.message", "x"), ("s0.opt.queue", "Bad Queue"), ("s0.opt.priority", "500"), ("op", "enqueue")));
        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
        var bad = await invalid.Content.ReadAsStringAsync();
        Assert.Contains("class=\"fielderr\">Queue &quot;Bad Queue&quot; is not a valid name", bad);
        Assert.Contains("Priority must be a whole number from -100 to 100.", bad);

        var posted = await http.PostAsync("/jobs/chain", Form(
            ("after.id", anchor), ("after.condition", "on_finished"), ("steps", "2"),
            ("s0.job", "log-message"), ("s0.arg.message", "one"), ("s0.opt.priority", "7"), ("s0.opt.maxAttempts", "4"),
            ("s1.job", "succeed-after"), ("s1.arg.delayMs", "0"), ("s1.cond", "on_succeeded"),
            ("s1.opt.idempotencyKey", "chain-step-2"), ("s1.opt.retryDelayMs", "250"), ("s1.opt.retryBackoff", "exponential"),
            ("op", "enqueue")));
        Assert.Equal(HttpStatusCode.Redirect, posted.StatusCode);
        var ids = posted.Headers.Location!.OriginalString.Split('=')[1].Split(',');
        var first = await demo.Api.GetJobAsync(ids[0]);
        var second = await demo.Api.GetJobAsync(ids[1]);
        Assert.Equal(("awaiting", anchor, "on_finished", 7, 4), (first.State, first.ContinueAfter, first.ContinuationCondition, first.Priority, first.MaxAttempts));
        Assert.Equal(("awaiting", ids[0], "on_succeeded", "chain-step-2", 250, "exponential"),
            (second.State, second.ContinueAfter, second.ContinuationCondition, second.IdempotencyKey, second.RetryDelayMs, second.RetryBackoff));
    }
}
