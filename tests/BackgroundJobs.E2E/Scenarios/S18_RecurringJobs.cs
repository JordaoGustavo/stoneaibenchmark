using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using BackgroundJobs.E2E.Infra;
using Npgsql;

namespace BackgroundJobs.E2E.Scenarios;

[Collection(E2ECollection.Name)]
public sealed class S18_RecurringJobs(PostgresFixture pg)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Every_second_cron_creates_one_job_per_occurrence_until_removed(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = Http(demo);

        var put = await http.PutAsJsonAsync("/api/recurring/tick", new
        {
            job = "log-message", cron = "* * * * * *", args = new { message = "tick" },
            options = new { queue = "default", priority = 5, retryJitterMs = 250 },
        }, Json);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        var view = await put.Content.ReadFromJsonAsync<JsonObject>(Json);
        Assert.Equal("tick", (string?)view!["id"]);
        Assert.Equal("UTC", (string?)view["timeZone"]);
        Assert.Equal(250, view["retryJitterMs"]!.GetValue<int>());

        var jobs = await WaitForJobsAsync(demo, "log-message", n => n >= 3, TimeSpan.FromSeconds(6));
        var job = await demo.Api.WaitForStateAsync(jobs[0], "succeeded");
        Assert.Equal("tick", job.Args.GetProperty("message").GetString());
        Assert.Equal(5, job.Priority);

        var got = await http.GetFromJsonAsync<JsonObject>("/api/recurring/tick", Json);
        Assert.NotNull((string?)got!["lastJobId"]);
        Assert.True(DateTime.Parse((string)got["nextRunAt"]!).ToUniversalTime() > DateTime.UtcNow.AddSeconds(-1));

        var list = await http.GetFromJsonAsync<JsonObject>("/api/recurring", Json);
        Assert.Equal("tick", (string?)list!["items"]![0]!["id"]);

        Assert.Equal(HttpStatusCode.OK, (await http.DeleteAsync("/api/recurring/tick")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await http.GetAsync("/api/recurring/tick")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await http.DeleteAsync("/api/recurring/tick")).StatusCode);
        await Task.Delay(300);
        var afterRemove = (await JobIdsAsync(demo, "log-message")).Count;
        await Task.Delay(2000);
        Assert.Equal(afterRemove, (await JobIdsAsync(demo, "log-message")).Count);
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Next_run_follows_the_time_zone_and_trigger_does_not_move_it(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = Http(demo);

        // Mondays 09:00 in São Paulo (UTC-3, no DST since 2019) is Mondays 12:00 UTC.
        var put = await http.PutAsJsonAsync("/api/recurring/weekly", new
        {
            job = "succeed-after", cron = "0 9 * * MON", timeZone = "America/Sao_Paulo", args = new { delayMs = 0 },
        }, Json);
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        var next = DateTime.Parse((string)(await put.Content.ReadFromJsonAsync<JsonObject>(Json))!["nextRunAt"]!).ToUniversalTime();
        Assert.Equal(NextMondayNoonUtc(DateTime.UtcNow), next);

        var trigger = await http.PostAsync("/api/recurring/weekly/trigger", null);
        Assert.Equal(HttpStatusCode.Accepted, trigger.StatusCode);
        var jobId = (string)(await trigger.Content.ReadFromJsonAsync<JsonObject>(Json))!["jobId"]!;
        await demo.Api.WaitForStateAsync(jobId, "succeeded");

        var got = await http.GetFromJsonAsync<JsonObject>("/api/recurring/weekly", Json);
        Assert.Equal(next, DateTime.Parse((string)got!["nextRunAt"]!).ToUniversalTime());
        Assert.Equal(jobId, (string?)got["lastJobId"]);

        // Same cron again: the pending occurrence is kept.
        await http.PutAsJsonAsync("/api/recurring/weekly", new
        {
            job = "succeed-after", cron = "0 9 * * MON", timeZone = "America/Sao_Paulo", args = new { delayMs = 5 },
        }, Json);
        got = await http.GetFromJsonAsync<JsonObject>("/api/recurring/weekly", Json);
        Assert.Equal(next, DateTime.Parse((string)got!["nextRunAt"]!).ToUniversalTime());
        Assert.Equal(5, got["args"]!["delayMs"]!.GetValue<int>());
    }

    [Theory]
    [MemberData(nameof(Stores.All), MemberType = typeof(Stores))]
    public async Task Invalid_definitions_are_rejected(string store)
    {
        await using var demo = await DemoProcess.StartAsync(await pg.CreateStoreAsync(store));
        using var http = Http(demo);

        async Task Rejected(string id, object body)
        {
            var res = await http.PutAsJsonAsync($"/api/recurring/{id}", body, Json);
            Assert.True(res.StatusCode == HttpStatusCode.BadRequest, $"{id}: {res.StatusCode} {await res.Content.ReadAsStringAsync()}");
        }

        await Rejected("bad-cron", new { job = "log-message", cron = "61 * * * *", args = new { message = "x" } });
        await Rejected("bad-fields", new { job = "log-message", cron = "* * *", args = new { message = "x" } });
        await Rejected("never", new { job = "log-message", cron = "0 0 30 2 *", args = new { message = "x" } });
        await Rejected("bad-tz", new { job = "log-message", cron = "* * * * *", timeZone = "Mars/Olympus", args = new { message = "x" } });
        await Rejected("bad-queue", new { job = "log-message", cron = "* * * * *", args = new { message = "x" }, options = new { queue = "Bad Queue" } });
        await Rejected("unknown-job", new { job = "no-such-job", cron = "* * * * *", args = new { } });
        await Rejected("bad%20id", new { job = "log-message", cron = "* * * * *", args = new { message = "x" } });
        Assert.Equal(HttpStatusCode.NotFound, (await http.PostAsync("/api/recurring/missing/trigger", null)).StatusCode);
    }

    [Fact]
    public async Task Definitions_survive_restarts_and_two_nodes_fire_each_occurrence_once()
    {
        var db = await pg.CreateDatabaseAsync();
        // Seeded at startup by the demo (RECURRING_HEARTBEAT_CRON → AddRecurringJob).
        var env = new Dictionary<string, string> { ["RECURRING_HEARTBEAT_CRON"] = "*/2 * * * * *" };
        await using (var first = await DemoProcess.StartAsync(db, env: env))
        {
            using var http = Http(first);
            var seeded = await http.GetFromJsonAsync<JsonObject>("/api/recurring/heartbeat", Json);
            Assert.Equal("*/2 * * * * *", (string?)seeded!["cron"]);
            await http.DeleteAsync("/api/recurring/heartbeat");
            await http.PutAsJsonAsync("/api/recurring/pulse", new { job = "succeed-after", cron = "* * * * * *", args = new { delayMs = 0 } }, Json);
            await first.KillAsync();
        }

        await using var a = await DemoProcess.StartAsync(db, env: new Dictionary<string, string> { ["NODE_ID"] = "a" });
        await using var b = await DemoProcess.StartAsync(db, env: new Dictionary<string, string> { ["NODE_ID"] = "b" });
        var before = (await JobIdsAsync(a, "succeed-after")).Count;
        var started = DateTime.UtcNow;
        await Task.Delay(4000);
        var created = (await JobIdsAsync(a, "succeed-after")).Count - before;
        var seconds = (DateTime.UtcNow - started).TotalSeconds;
        // One job per second, not one per node per second.
        Assert.InRange(created, 3, (int)Math.Ceiling(seconds) + 1);
    }

    [Fact]
    public async Task Definitions_this_node_cannot_schedule_do_not_hold_up_the_others()
    {
        var db = await pg.CreateDatabaseAsync();
        await using (var setup = await DemoProcess.StartAsync(db)) { }

        // More unusable due definitions than one scheduler batch, all due before the good one.
        await using (var conn = new NpgsqlConnection(db))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                INSERT INTO bj_recurring (id, job_name, cron, time_zone, args, args_version, max_attempts, retry_delay_ms,
                                          retry_jitter_ms, queue, priority, next_run_at, created_at, updated_at)
                SELECT 'stuck-' || i, 'log-message', '* * * * *', 'Mars/Olympus', '{"message":"x"}', 1, 1, 0, 0, 'default', 0,
                       now() - interval '1 hour', now(), now()
                FROM generate_series(1, 150) i
                """, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        await using var demo = await DemoProcess.StartAsync(db);
        using var http = Http(demo);
        await http.PutAsJsonAsync("/api/recurring/good", new { job = "succeed-after", cron = "* * * * * *", args = new { delayMs = 0 } }, Json);
        await WaitForJobsAsync(demo, "succeed-after", n => n >= 2, TimeSpan.FromSeconds(6));
        Assert.Empty(await JobIdsAsync(demo, "log-message"));
    }

    private static DateTime NextMondayNoonUtc(DateTime now)
    {
        var day = now.Date;
        while (true)
        {
            var candidate = day.AddHours(12);
            if (day.DayOfWeek == DayOfWeek.Monday && candidate > now) return DateTime.SpecifyKind(candidate, DateTimeKind.Utc);
            day = day.AddDays(1);
        }
    }

    private static HttpClient Http(DemoProcess demo) => new() { BaseAddress = demo.Api.BaseAddress, Timeout = TimeSpan.FromSeconds(10) };

    private static async Task<List<string>> JobIdsAsync(DemoProcess demo, string name)
    {
        var body = await (await demo.Api.ListRawAsync()).Content.ReadFromJsonAsync<JsonObject>(Json);
        return body!["items"]!.AsArray().Where(j => (string?)j!["name"] == name).Select(j => (string)j!["id"]!).ToList();
    }

    private static async Task<List<string>> WaitForJobsAsync(DemoProcess demo, string name, Func<int, bool> until, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        List<string> ids;
        do
        {
            ids = await JobIdsAsync(demo, name);
            if (until(ids.Count)) return ids;
            await Task.Delay(100);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException($"only {ids.Count} {name} jobs");
    }
}
