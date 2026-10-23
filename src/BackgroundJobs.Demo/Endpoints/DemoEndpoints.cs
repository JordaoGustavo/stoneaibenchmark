using System.Text.Json;
using System.Text.Json.Serialization;
using BackgroundJobs.Demo.Events;
using BackgroundJobs.Demo.Jobs;
using BackgroundJobs.Demo.Telemetry;
using Npgsql;

namespace BackgroundJobs.Demo.Endpoints;

/// The demo's Postgres connection string, for its own tables; null on the in-memory store.
public sealed record DemoDatabase(string? ConnectionString);

public static class DemoEndpoints
{
    private sealed record CreateJobRequest(string Job, JsonElement Params, CreateJobOptions? Options = null);

    private sealed record CreateBatchRequest(List<CreateJobRequest> Jobs);

    private sealed record CreateOrderRequest(string OrderId, List<CreateJobRequest> Jobs, bool Rollback = false);

    private sealed record CreateJobOptions(
        DateTimeOffset? RunAt = null, int? MaxAttempts = null, int? RetryDelayMs = null, string? Queue = null, int? Priority = null, string? IdempotencyKey = null);

    /// Strict input: unknown fields, missing parameters, nulls and numbers-as-strings are rejected.
    private static readonly JsonSerializerOptions Strict = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true,
        NumberHandling = JsonNumberHandling.Strict,
    };

    private sealed class BadRequest(string message) : Exception(message);

    public static IEndpointRouteBuilder MapDemoEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/demo/events", (EventLog events) => Results.Ok(new { items = events.All() }));

        app.MapGet("/demo/telemetry", (TelemetryRecorder telemetry) =>
        {
            var (spans, measurements) = telemetry.Snapshot();
            return Results.Ok(new { spans, measurements });
        });

        app.MapPost("/demo/jobs", async (HttpRequest http, IJobClient jobs, JobRegistry registry, CancellationToken ct) =>
        {
            try
            {
                var request = await JsonSerializer.DeserializeAsync<CreateJobRequest>(http.Body, Strict, ct)
                              ?? throw new BadRequest("body is required");
                var job = ToJob(request, registry);
                var id = await jobs.EnqueueAsync(job.JobName, job.Args, job.Options, ct);
                return Results.Accepted(value: new { jobId = id.ToString() });
            }
            catch (Exception e) when (e is JsonException or BadRequest or ArgumentException)
            {
                return ManagementEndpoints.Error(400, e.Message);
            }
        });

        // Batch enqueue: all jobs or none.
        app.MapPost("/demo/jobs/batch", async (HttpRequest http, IJobClient jobs, JobRegistry registry, CancellationToken ct) =>
        {
            try
            {
                var request = await JsonSerializer.DeserializeAsync<CreateBatchRequest>(http.Body, Strict, ct)
                              ?? throw new BadRequest("body is required");
                var ids = await jobs.EnqueueManyAsync(request.Jobs.Select(j => ToJob(j, registry)).ToList(), ct);
                return Results.Accepted(value: new { jobIds = ids.Select(id => id.ToString()) });
            }
            catch (Exception e) when (e is JsonException or BadRequest or ArgumentException)
            {
                return ManagementEndpoints.Error(400, e.Message);
            }
        });

        // Transactional enqueue (outbox): writes an order row and its jobs on one transaction, then commits it, or rolls it
        // back when rollback is true. Postgres only.
        app.MapPost("/demo/orders", async (HttpRequest http, IJobClient jobs, JobRegistry registry, DemoDatabase database, CancellationToken ct) =>
        {
            try
            {
                var request = await JsonSerializer.DeserializeAsync<CreateOrderRequest>(http.Body, Strict, ct)
                              ?? throw new BadRequest("body is required");
                if (database.ConnectionString == null) throw new BadRequest("orders need the postgres store");
                var batch = request.Jobs.Select(j => ToJob(j, registry)).ToList();

                await using var conn = new NpgsqlConnection(database.ConnectionString);
                await conn.OpenAsync(ct);
                await using (var create = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS demo_orders (id text PRIMARY KEY)", conn))
                    await create.ExecuteNonQueryAsync(ct);
                await using var tx = await conn.BeginTransactionAsync(ct);
                await using (var insert = new NpgsqlCommand("INSERT INTO demo_orders (id) VALUES (@id)", conn, tx))
                {
                    insert.Parameters.AddWithValue("id", request.OrderId);
                    await insert.ExecuteNonQueryAsync(ct);
                }
                var ids = await jobs.EnqueueManyAsync(tx, batch, ct);
                if (request.Rollback) await tx.RollbackAsync(ct);
                else await tx.CommitAsync(ct);
                return Results.Ok(new { jobIds = ids.Select(id => id.ToString()) });
            }
            catch (Exception e) when (e is JsonException or BadRequest or ArgumentException)
            {
                return ManagementEndpoints.Error(400, e.Message);
            }
        });

        return app;
    }

    private static JobToEnqueue ToJob(CreateJobRequest request, JobRegistry registry)
    {
        var options = ParseOptions(request.Options);
        object args = request.Job switch
        {
            SucceedAfterJob.Name => Validate(Parse<SucceedAfterArgs>(request.Params)),
            FailNTimesJob.Name => Validate(Parse<FailNTimesArgs>(request.Params)),
            LogMessageJob.Name => Validate(Parse<LogMessageArgs>(request.Params)),
            SleepJob.Name => Validate(Parse<SleepArgs>(request.Params)),
            // Whichever greet args this process registered (see GREET_ARGS_VERSION).
            GreetJob.Name when registry.Registrations.Contains((GreetJob.Name, typeof(GreetV1Args))) => Parse<GreetV1Args>(request.Params),
            GreetJob.Name => Parse<GreetArgs>(request.Params),
            _ => throw new BadRequest($"unknown job '{request.Job}'"),
        };
        return new JobToEnqueue(request.Job, args, options);
    }

    private static T Parse<T>(JsonElement json) =>
        json.ValueKind == JsonValueKind.Object ? json.Deserialize<T>(Strict)! : throw new BadRequest("params must be an object");

    private static JobOptions? ParseOptions(CreateJobOptions? o)
    {
        if (o == null) return null;
        if (o.MaxAttempts is < 1 or > 10) throw new BadRequest("options.maxAttempts must be between 1 and 10");
        if (o.RetryDelayMs is < 0 or > 60_000) throw new BadRequest("options.retryDelayMs must be between 0 and 60000");
        return new JobOptions
        {
            RunAt = o.RunAt,
            MaxAttempts = o.MaxAttempts,
            RetryDelay = o.RetryDelayMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
            // Queue and priority are checked by the library (ArgumentException → 400).
            Queue = o.Queue,
            Priority = o.Priority,
            IdempotencyKey = o.IdempotencyKey,
        };
    }

    private static SucceedAfterArgs Validate(SucceedAfterArgs a) =>
        a.DelayMs is >= 0 and <= 60_000 ? a : throw new BadRequest("params.delayMs must be between 0 and 60000");

    private static FailNTimesArgs Validate(FailNTimesArgs a) =>
        a.FailTimes is >= 0 and <= 10 ? a : throw new BadRequest("params.failTimes must be between 0 and 10");

    private static SleepArgs Validate(SleepArgs a) =>
        a.DelayMs is >= 0 and <= 60_000 ? a : throw new BadRequest("params.delayMs must be between 0 and 60000");

    private static LogMessageArgs Validate(LogMessageArgs a) =>
        a.Message.Length <= 200 ? a : throw new BadRequest("params.message must have at most 200 characters");
}
