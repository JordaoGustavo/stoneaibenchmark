using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S12_Observability(PostgresFixture pg)
{
    private static Dictionary<string, string> Env(params (string Key, string Value)[] pairs) => pairs.ToDictionary(p => p.Key, p => p.Value);

    private static async Task<JsonObject> GetJsonAsync(DemoProcess demo, string path)
    {
        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };
        // Any status: /health/details answers 503 with a body when unhealthy.
        return (await (await http.GetAsync(path)).Content.ReadFromJsonAsync<JsonObject>())!;
    }

    private static async Task<JsonObject> WaitForJsonAsync(DemoProcess demo, string path, Func<JsonObject, bool> until)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        JsonObject body;
        do
        {
            body = await GetJsonAsync(demo, path);
            if (until(body)) return body;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException($"{path} never matched: {body.ToJsonString()}");
    }

    private static IEnumerable<JsonNode> Measurements(JsonObject telemetry, string instrument) =>
        telemetry["measurements"]!.AsArray().Where(m => m!["instrument"]!.GetValue<string>() == instrument)!;

    [Fact]
    public async Task Each_attempt_emits_a_span_and_duration_and_outcome_metrics()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        var id = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 1 }, new { retryDelayMs = 0 });
        await demo.Api.WaitForStateAsync(id, "succeeded");

        var telemetry = await WaitForJsonAsync(demo, "/demo/telemetry", t => t["spans"]!.AsArray().Count >= 2);
        var spans = telemetry["spans"]!.AsArray().Where(s => s!["tags"]!["job.id"]!.GetValue<string>() == id).ToList();
        Assert.Equal([1, 2], spans.Select(s => s!["tags"]!["job.attempt"]!.GetValue<int>()));
        Assert.All(spans, s =>
        {
            Assert.Equal("job fail-n-times", s!["name"]!.GetValue<string>());
            Assert.Equal("fail-n-times", s["tags"]!["job.name"]!.GetValue<string>());
            Assert.Equal(1, s["tags"]!["job.run"]!.GetValue<int>());
        });
        Assert.Equal(["Error", "Unset"], spans.Select(s => s!["status"]!.GetValue<string>()));
        Assert.Equal("demo failure", spans[0]!["statusDescription"]!.GetValue<string>());
        Assert.Equal(["failed", "succeeded"], spans.Select(s => s!["tags"]!["job.outcome"]!.GetValue<string>()));

        var attempts = Measurements(telemetry, "backgroundjobs.attempts").ToList();
        Assert.Equal(["failed", "succeeded"], attempts.Select(m => m["tags"]!["outcome"]!.GetValue<string>()));
        Assert.All(attempts, m => Assert.Equal(1, m["value"]!.GetValue<double>()));
        var durations = Measurements(telemetry, "backgroundjobs.attempt.duration").ToList();
        Assert.Equal(2, durations.Count);
        Assert.All(durations, m =>
        {
            Assert.Equal("s", m["unit"]!.GetValue<string>());
            Assert.Equal("fail-n-times", m["tags"]!["job.name"]!.GetValue<string>());
            Assert.InRange(m["value"]!.GetValue<double>(), 0, 5);
        });
    }

    [Fact]
    public async Task Queue_depth_and_oldest_ready_age_are_reported_as_gauges()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync(), workers: 1, env: Env(("QUEUE_MONITOR_MS", "100")));
        var blocker = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 });
        await demo.Api.WaitForStateAsync(blocker, "processing");
        await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });
        await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });

        var telemetry = await WaitForJsonAsync(demo, "/demo/telemetry", t =>
            Measurements(t, "backgroundjobs.queue.depth").Any(m => m["value"]!.GetValue<double>() == 2)
            && Measurements(t, "backgroundjobs.queue.oldest_age").Any(m => m["value"]!.GetValue<double>() >= 0.5));
        Assert.Equal("s", Measurements(telemetry, "backgroundjobs.queue.oldest_age").Single()["unit"]!.GetValue<string>());

        await WaitForJsonAsync(demo, "/demo/telemetry", t =>
            Measurements(t, "backgroundjobs.queue.depth").Any(m => m["value"]!.GetValue<double>() == 0)
            && Measurements(t, "backgroundjobs.queue.oldest_age").Any(m => m["value"]!.GetValue<double>() == 0));
    }

    [Fact]
    public async Task Every_log_written_during_an_attempt_carries_job_id_attempt_and_run_in_scope()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync(), env: Env(
            ("Logging__Console__FormatterName", "json"),
            ("Logging__Console__FormatterOptions__IncludeScopes", "true")));
        var ok = await demo.Api.EnqueueAsync("log-message", new { message = "hello-scope" });
        var failing = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 5 }, new { maxAttempts = 1 });
        await demo.Api.WaitForStateAsync(ok, "succeeded");
        await demo.Api.WaitForStateAsync(failing, "failed");

        var success = await WaitForLogAsync(demo, l => l.GetProperty("Message").GetString()!.Contains("hello-scope"));
        AssertJobScope(success, ok, attempt: 1, run: 1);
        var failure = await WaitForLogAsync(demo, l => l.GetProperty("Message").GetString()!.Contains("failed: demo failure"));
        AssertJobScope(failure, failing, attempt: 1, run: 1);
    }

    private static void AssertJobScope(JsonElement log, string jobId, int attempt, int run)
    {
        var scope = log.GetProperty("Scopes").EnumerateArray().Single(s => s.ValueKind == JsonValueKind.Object && s.TryGetProperty("JobId", out _));
        Assert.Equal(jobId, scope.GetProperty("JobId").GetString());
        Assert.Equal(attempt, scope.GetProperty("Attempt").GetInt32());
        Assert.Equal(run, scope.GetProperty("Run").GetInt32());
    }

    private static async Task<JsonElement> WaitForLogAsync(DemoProcess demo, Func<JsonElement, bool> match)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        do
        {
            foreach (var line in demo.Output.Split('\n').Where(l => l.StartsWith('{')))
            {
                var log = JsonDocument.Parse(line).RootElement;
                if (match(log)) return log;
            }
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException("log line not found:\n" + demo.Output);
    }

    [Fact]
    public async Task Health_details_reports_a_stalled_queue_while_readiness_stays_ok()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync(), workers: 1, env: Env(
            ("QUEUE_MONITOR_MS", "100"), ("STALLED_QUEUE_SECONDS", "1")));
        var healthy = await GetJsonAsync(demo, "/health/details");
        Assert.Equal("healthy", healthy["status"]!.GetValue<string>());
        Assert.True(healthy["ready"]!.GetValue<bool>());

        var blocker = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 4000 });
        await demo.Api.WaitForStateAsync(blocker, "processing");
        var waiting = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 0 });

        var stalled = await WaitForJsonAsync(demo, "/health/details", h => h["status"]!.GetValue<string>() == "degraded");
        Assert.True(stalled["queue"]!["stalled"]!.GetValue<bool>());
        Assert.Equal(1, stalled["queue"]!["ready"]!.GetValue<int>());
        Assert.True(stalled["queue"]!["oldestReadyAgeSeconds"]!.GetValue<double>() > 1);
        Assert.Equal(1, stalled["queue"]!["stalledThresholdSeconds"]!.GetValue<double>());
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.GetHealthAsync()).StatusCode);

        await demo.Api.WaitForStateAsync(waiting, "succeeded");
        await WaitForJsonAsync(demo, "/health/details", h => h["status"]!.GetValue<string>() == "healthy");
    }

    [Fact]
    public async Task Health_details_reports_circuit_breaker_state_per_job_type()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync(), env: Env(
            ("FAIL_BREAKER_THRESHOLD", "1"), ("FAIL_BREAKER_SECONDS", "30")));
        var closed = await GetJsonAsync(demo, "/health/details");
        var breaker = closed["circuitBreakers"]!.AsArray().Single()!;
        Assert.Equal("fail-n-times", breaker["job"]!.GetValue<string>());
        Assert.Equal("closed", breaker["state"]!.GetValue<string>());

        var id = await demo.Api.EnqueueAsync("fail-n-times", new { failTimes = 5 }, new { maxAttempts = 1 });
        await demo.Api.WaitForStateAsync(id, "failed");

        var open = await GetJsonAsync(demo, "/health/details");
        Assert.Equal("degraded", open["status"]!.GetValue<string>());
        breaker = open["circuitBreakers"]!.AsArray().Single()!;
        Assert.Equal("open", breaker["state"]!.GetValue<string>());
        Assert.Equal(1, breaker["consecutiveFailures"]!.GetValue<int>());
        Assert.True(breaker["openUntil"]!.GetValue<DateTime>() > DateTime.UtcNow.AddSeconds(20));
    }

    [Fact]
    public async Task Health_details_is_503_once_stop_is_requested()
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateDatabaseAsync());
        var id = await demo.Api.EnqueueAsync("succeed-after", new { delayMs = 3000 });
        await demo.Api.WaitForStateAsync(id, "processing");
        demo.Terminate();

        using var http = new HttpClient { BaseAddress = demo.Api.BaseAddress };
        var deadline = DateTime.UtcNow.AddSeconds(5);
        HttpResponseMessage res;
        while ((res = await http.GetAsync("/health/details")).StatusCode != HttpStatusCode.ServiceUnavailable && DateTime.UtcNow < deadline)
            await Task.Delay(50);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, res.StatusCode);
        var body = (await res.Content.ReadFromJsonAsync<JsonObject>())!;
        Assert.Equal("unhealthy", body["status"]!.GetValue<string>());
        Assert.False(body["ready"]!.GetValue<bool>());
    }

    [Fact]
    public async Task Health_details_reports_jobs_running_without_a_worker()
    {
        await using var demo = await DemoProcess.StartAsync(null, workers: 1, env: Env(
            ("JOB_TIMEOUT_MS", "300"), ("TIMEOUT_GRACE_MS", "200"), ("MAX_ABANDONED_JOBS", "2"), ("ABANDONED_ALERT_MS", "1500")));

        var first = await demo.Api.EnqueueAsync("sleep", new { delayMs = 6000, ignoreCancellation = true });
        var abandoned = await WaitForJsonAsync(demo, "/health/details", h => h["abandoned"]!["count"]!.GetValue<int>() == 1);
        // Freshly abandoned: reported, but not yet alarming.
        Assert.Equal("healthy", abandoned["status"]!.GetValue<string>());
        Assert.Equal(first, abandoned["abandoned"]!["jobs"]![0]!["id"]!.GetValue<string>());
        Assert.Equal("sleep", abandoned["abandoned"]!["jobs"]![0]!["name"]!.GetValue<string>());

        var old = await WaitForJsonAsync(demo, "/health/details", h => h["status"]!.GetValue<string>() == "degraded");
        Assert.True(old["abandoned"]!["alert"]!.GetValue<bool>());

        // At the limit the node is about to lose workers for good: unhealthy, while readiness stays up.
        var second = await demo.Api.EnqueueAsync("sleep", new { delayMs = 6000, ignoreCancellation = true });
        var full = await WaitForJsonAsync(demo, "/health/details", h => h["abandoned"]!["full"]!.GetValue<bool>());
        Assert.Equal("unhealthy", full["status"]!.GetValue<string>());
        Assert.Equal(HttpStatusCode.OK, (await demo.Api.GetHealthAsync()).StatusCode);

        await demo.Api.WaitForStateAsync(first, "succeeded", TimeSpan.FromSeconds(10));
        await demo.Api.WaitForStateAsync(second, "succeeded", TimeSpan.FromSeconds(10));
        await WaitForJsonAsync(demo, "/health/details", h => h["status"]!.GetValue<string>() == "healthy");
    }
}
