using System.Text.Json;
using BackgroundJobs.Health;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;

namespace BackgroundJobs;

public static class ManagementEndpoints
{
    private sealed record UpsertRecurringRequest(string Job, string Cron, JsonElement Args, string? TimeZone = null, RecurringRequestOptions? Options = null);

    private sealed record RecurringRequestOptions(string? Queue = null, int? Priority = null, int? MaxAttempts = null, int? RetryDelayMs = null, int? RetryJitterMs = null);

    /// Maps /api/jobs (list, get, delete, requeue, cancel), /api/recurring (list, get, put, delete, trigger), /health and
    /// /health/details. Opt-in.
    public static IEndpointRouteBuilder MapBackgroundJobsApi(this IEndpointRouteBuilder app)
    {
        app.MapGet("/health", (JobServerHealthCheck health) => health.IsReady
            ? Results.Ok(new { status = "ok" })
            : Results.Json(new { status = "unavailable" }, statusCode: StatusCodes.Status503ServiceUnavailable));

        // 200 while healthy or degraded, 503 when not processing.
        app.MapGet("/health/details", (JobServerHealthCheck health) =>
        {
            var r = health.Report();
            return Results.Json(new
            {
                status = r.Status.ToString().ToLowerInvariant(),
                ready = r.Ready,
                queue = new
                {
                    ready = r.Queue.Ready,
                    oldestReadyAgeSeconds = r.Queue.OldestReadyAge?.TotalSeconds,
                    stalled = r.Queue.Stalled,
                    stalledThresholdSeconds = r.Queue.StalledThreshold.TotalSeconds,
                    sampledAt = r.Queue.SampledAt?.UtcDateTime,
                    queues = r.Queue.Queues.Select(q => new
                    {
                        name = q.Queue, consumed = q.Consumed, orphaned = q.Orphaned, ready = q.Ready, oldestReadyAgeSeconds = q.OldestReadyAge.TotalSeconds,
                    }),
                },
                circuitBreakers = r.CircuitBreakers.Select(b => new
                {
                    job = b.Job, state = b.State, consecutiveFailures = b.ConsecutiveFailures, openUntil = b.OpenUntil?.UtcDateTime,
                }),
                abandoned = new
                {
                    count = r.Abandoned.Jobs.Count,
                    limit = r.Abandoned.Limit,
                    full = r.Abandoned.Full,
                    alert = r.Abandoned.Alert,
                    alertAfterSeconds = r.Abandoned.AlertAfter.TotalSeconds,
                    jobs = r.Abandoned.Jobs.Select(j => new { id = j.Id.ToString(), name = j.Name, since = j.Since.UtcDateTime, ageSeconds = j.Age.TotalSeconds }),
                },
            }, statusCode: r.Status == HealthStatus.Unhealthy ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status200OK);
        });

        var jobs = app.MapGroup("/api/jobs");

        jobs.MapGet("/", async (IJobClient client, string? after, int? limit, CancellationToken ct) =>
        {
            Guid? cursor = null;
            if (after != null)
            {
                if (!Guid.TryParse(after, out var parsed)) return Error(400, "after must be a job id");
                cursor = parsed;
            }
            var pageSize = limit ?? 1000;
            if (pageSize is < 1 or > 1000) return Error(400, "limit must be between 1 and 1000");
            var page = await client.ListAsync(cursor, pageSize, ct);
            return Results.Ok(new
            {
                items = page.Select(j => new { id = j.Id.ToString(), name = j.Name, state = j.State.ToWire(), attempts = j.Attempts, queue = j.Queue, priority = j.Priority }),
                nextCursor = page.Count == pageSize ? page[^1].Id.ToString() : null,
            });
        });

        jobs.MapGet("/{id}", (IJobClient client, string id, CancellationToken ct) =>
            WithJob(id, async jobId => Results.Ok(ToView(await client.GetAsync(jobId, ct)))));

        jobs.MapDelete("/{id}", (IJobClient client, string id, CancellationToken ct) =>
            WithJob(id, async jobId =>
            {
                await client.DeleteAsync(jobId, ct);
                return Results.Ok(new { id = jobId.ToString(), state = "deleted" });
            }));

        jobs.MapPost("/{id}/requeue", (IJobClient client, string id, CancellationToken ct) =>
            WithJob(id, async jobId =>
            {
                await client.RequeueAsync(jobId, ct);
                return Results.Accepted(value: new { jobId = jobId.ToString() });
            }));

        // Accepted, not OK: a processing job ends cancelled only once its code has stopped.
        jobs.MapPost("/{id}/cancel", (IJobClient client, string id, CancellationToken ct) =>
            WithJob(id, async jobId =>
            {
                await client.CancelAsync(jobId, ct);
                return Results.Accepted(value: new { jobId = jobId.ToString() });
            }));

        var recurring = app.MapGroup("/api/recurring");

        recurring.MapGet("/", async (IRecurringJobClient client, CancellationToken ct) =>
            Results.Ok(new { items = (await client.ListAsync(ct)).Select(ToView) }));

        recurring.MapGet("/{id}", (IRecurringJobClient client, string id, CancellationToken ct) =>
            WithRecurring(async () => Results.Ok(ToView(await client.GetAsync(id, ct)))));

        // Creates or replaces; the pending occurrence is kept when cron and time zone are unchanged.
        recurring.MapPut("/{id}", async (IRecurringJobClient client, string id, HttpRequest http, CancellationToken ct) =>
        {
            try
            {
                var request = await JsonSerializer.DeserializeAsync<UpsertRecurringRequest>(http.Body, JsonSerializerOptions.Web, ct);
                if (request?.Job == null || request.Cron == null) return Error(400, "job and cron are required");
                if (request.Args.ValueKind != JsonValueKind.Object) return Error(400, "args must be an object");
                var o = request.Options;
                var options = new RecurringJobOptions
                {
                    TimeZone = request.TimeZone == null ? null : TimeZoneInfo.FindSystemTimeZoneById(request.TimeZone),
                    Queue = o?.Queue,
                    Priority = o?.Priority,
                    MaxAttempts = o?.MaxAttempts,
                    RetryDelay = o?.RetryDelayMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
                    RetryJitter = o?.RetryJitterMs is { } jitterMs ? TimeSpan.FromMilliseconds(jitterMs) : null,
                };
                return Results.Ok(ToView(await client.UpsertAsync(id, request.Job, request.Cron, request.Args, options, ct)));
            }
            catch (Exception e) when (e is JsonException or ArgumentException or TimeZoneNotFoundException or InvalidTimeZoneException)
            {
                return Error(400, e.Message);
            }
        });

        recurring.MapDelete("/{id}", (IRecurringJobClient client, string id, CancellationToken ct) =>
            WithRecurring(async () =>
            {
                await client.RemoveAsync(id, ct);
                return Results.Ok(new { id });
            }));

        recurring.MapPost("/{id}/trigger", (IRecurringJobClient client, string id, CancellationToken ct) =>
            WithRecurring(async () => Results.Accepted(value: new { jobId = (await client.TriggerAsync(id, ct)).ToString() })));

        return app;
    }

    private static async Task<IResult> WithRecurring(Func<Task<IResult>> action)
    {
        try { return await action(); }
        catch (RecurringJobNotFoundException e) { return Error(404, e.Message); }
    }

    private static object ToView(RecurringJob r) => new
    {
        id = r.Id,
        job = r.JobName,
        cron = r.Cron,
        timeZone = r.TimeZone,
        args = r.Args,
        queue = r.Queue,
        priority = r.Priority,
        maxAttempts = r.MaxAttempts,
        retryDelayMs = (int)r.RetryDelay.TotalMilliseconds,
        retryJitterMs = (int)r.RetryJitter.TotalMilliseconds,
        nextRunAt = r.NextRunAt,
        lastRunAt = r.LastRunAt,
        lastJobId = r.LastJobId?.ToString(),
        createdAt = r.CreatedAt,
        updatedAt = r.UpdatedAt,
    };

    public static IResult Error(int status, string message) => Results.Json(new { error = message }, statusCode: status);

    private static async Task<IResult> WithJob(string id, Func<Guid, Task<IResult>> action)
    {
        if (!Guid.TryParse(id, out var jobId)) return Error(404, $"job {id} not found");
        try { return await action(jobId); }
        catch (JobNotFoundException e) { return Error(404, e.Message); }
        catch (InvalidJobTransitionException e) { return Error(409, e.Message); }
        catch (IdempotencyKeyConflictException e) { return Error(409, e.Message); }
    }

    private static object ToView(Job job) => new
    {
        id = job.Id.ToString(),
        name = job.Name,
        args = job.Args,
        state = job.State.ToWire(),
        attempts = job.Attempts,
        maxAttempts = job.MaxAttempts,
        queue = job.Queue,
        priority = job.Priority,
        idempotencyKey = job.IdempotencyKey,
        retryDelayMs = (int)job.RetryDelay.TotalMilliseconds,
        scheduledAt = job.ScheduledAt,
        createdAt = job.CreatedAt,
        updatedAt = job.UpdatedAt,
        history = job.History.Select(h => new { state = h.State.ToWire(), at = h.At, reason = h.Reason }),
        attemptHistory = job.AttemptHistory.Select(a => new
        {
            attempt = a.Attempt, startedAt = a.StartedAt, finishedAt = a.FinishedAt, outcome = a.Outcome?.ToWire(), error = a.Error,
        }),
    };
}
