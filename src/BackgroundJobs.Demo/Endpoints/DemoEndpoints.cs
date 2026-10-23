using System.Text.Json;
using System.Text.Json.Serialization;
using BackgroundJobs.Demo.Events;
using BackgroundJobs.Demo.Jobs;
using BackgroundJobs.Demo.Telemetry;

namespace BackgroundJobs.Demo.Endpoints;

public static class DemoEndpoints
{
    private sealed record CreateJobRequest(string Job, JsonElement Params, CreateJobOptions? Options = null);

    private sealed record CreateJobOptions(
        DateTimeOffset? RunAt = null, int? MaxAttempts = null, int? RetryDelayMs = null, string? Queue = null, int? Priority = null);

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
                var options = ParseOptions(request.Options);
                var id = request.Job switch
                {
                    SucceedAfterJob.Name => await jobs.EnqueueAsync(request.Job, Validate(Parse<SucceedAfterArgs>(request.Params)), options, ct),
                    FailNTimesJob.Name => await jobs.EnqueueAsync(request.Job, Validate(Parse<FailNTimesArgs>(request.Params)), options, ct),
                    LogMessageJob.Name => await jobs.EnqueueAsync(request.Job, Validate(Parse<LogMessageArgs>(request.Params)), options, ct),
                    SleepJob.Name => await jobs.EnqueueAsync(request.Job, Validate(Parse<SleepArgs>(request.Params)), options, ct),
                    // Whichever greet args this process registered (see GREET_ARGS_VERSION).
                    GreetJob.Name when registry.Registrations.Contains((GreetJob.Name, typeof(GreetV1Args))) =>
                        await jobs.EnqueueAsync(request.Job, Parse<GreetV1Args>(request.Params), options, ct),
                    GreetJob.Name => await jobs.EnqueueAsync(request.Job, Parse<GreetArgs>(request.Params), options, ct),
                    _ => throw new BadRequest($"unknown job '{request.Job}'"),
                };
                return Results.Accepted(value: new { jobId = id.ToString() });
            }
            catch (Exception e) when (e is JsonException or BadRequest or ArgumentException)
            {
                return ManagementEndpoints.Error(400, e.Message);
            }
        });

        return app;
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
