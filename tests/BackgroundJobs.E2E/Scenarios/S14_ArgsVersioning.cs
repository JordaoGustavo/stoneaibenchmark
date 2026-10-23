using BackgroundJobs.E2E.Infra;
using Npgsql;

namespace BackgroundJobs.E2E.Scenarios;

/// The demo's "greet" job changed its args: v1 {who}, v2 {firstName, lastName} with a registered v1→v2 upgrade.
/// GREET_ARGS_VERSION picks which code the process runs, simulating a deploy between enqueue and execution.
[Collection(E2ECollection.Name)]
public sealed class S14_ArgsVersioning(PostgresFixture pg)
{
    private static readonly Dictionary<string, string> V1 = new() { ["GREET_ARGS_VERSION"] = "1" };
    private static readonly Dictionary<string, string> V2 = new() { ["GREET_ARGS_VERSION"] = "2" };

    [Fact]
    public async Task Job_persisted_with_old_args_runs_after_deploy_through_the_registered_upgrade()
    {
        var db = await pg.CreateDatabaseAsync();
        var old = await DemoProcess.StartAsync(db, env: V1);
        var id = await old.Api.EnqueueAsync("greet", new { who = "Ada Lovelace" }, new { runAt = DateTime.UtcNow.AddSeconds(2) });
        Assert.Equal(1, await StoredArgsVersionAsync(db, id));
        await old.KillAsync();

        await using var deployed = await DemoProcess.StartAsync(db, env: V2);
        var done = await deployed.Api.WaitForStateAsync(id, "succeeded");
        // Stored args are never rewritten: the upgrade happens when the job is read for execution.
        Assert.Equal(1, await StoredArgsVersionAsync(db, id));
        Assert.Equal("Ada Lovelace", done.Args.GetProperty("who").GetString());
        var events = await deployed.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "message"));
        Assert.Equal("hello Ada (Lovelace)", events.Single(x => x.JobId == id && x.Type == "message").Detail!.Value.GetProperty("message").GetString());

        var current = await deployed.Api.EnqueueAsync("greet", new { firstName = "Grace", lastName = "Hopper" });
        await deployed.Api.WaitForStateAsync(current, "succeeded");
        Assert.Equal(2, await StoredArgsVersionAsync(db, current));
    }

    [Fact]
    public async Task Node_running_older_code_fails_a_job_with_newer_args_instead_of_misreading_it()
    {
        var db = await pg.CreateDatabaseAsync();
        var current = await DemoProcess.StartAsync(db, env: V2);
        var id = await current.Api.EnqueueAsync("greet", new { firstName = "Grace", lastName = "Hopper" },
            new { runAt = DateTime.UtcNow.AddSeconds(2), maxAttempts = 1 });
        await current.KillAsync();

        await using var rolledBack = await DemoProcess.StartAsync(db, env: V1);
        var failed = await rolledBack.Api.WaitForStateAsync(id, "failed");
        Assert.StartsWith("args version 2 of job 'greet' is newer than the registered version 1", failed.AttemptHistory.Single().Error);
        Assert.DoesNotContain(await rolledBack.Api.EventsAsync(), e => e.JobId == id);
    }

    [Fact]
    public async Task In_memory_store_runs_versioned_jobs()
    {
        await using var demo = await DemoProcess.StartAsync(connectionString: null, env: V2);
        var id = await demo.Api.EnqueueAsync("greet", new { firstName = "Alan", lastName = "Turing" });
        await demo.Api.WaitForStateAsync(id, "succeeded");
        var events = await demo.Api.WaitForEventsAsync(e => e.Any(x => x.JobId == id && x.Type == "message"));
        Assert.Equal("hello Alan (Turing)", events.Single(x => x.JobId == id && x.Type == "message").Detail!.Value.GetProperty("message").GetString());
    }

    // The version is storage metadata, not part of the HTTP job view the spec fixes.
    private static async Task<int> StoredArgsVersionAsync(string db, string id)
    {
        await using var conn = new NpgsqlConnection(db);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT args_version FROM bj_jobs WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", Guid.Parse(id));
        return (int)(await cmd.ExecuteScalarAsync())!;
    }
}
