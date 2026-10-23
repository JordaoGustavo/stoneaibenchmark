using System.Diagnostics;
using System.Text.Json;
using BackgroundJobs.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Runs one claimed job and records the outcome. Never throws: a job's failure must not affect others.
internal sealed class JobExecutor(
    IJobStore store,
    JobRegistry registry,
    JobCircuitBreaker jobBreaker,
    JobsMetrics metrics,
    IJobSignals signals,
    IServiceScopeFactory scopes,
    IOptions<BackgroundJobsOptions> options,
    TimeProvider time,
    ILogger<JobExecutor> logger)
{
    public async Task ExecuteAsync(ClaimedJob job)
    {
        // Everything logged during the attempt, the job's own logs included, carries these.
        using var logScope = logger.BeginScope(new Dictionary<string, object>
        {
            ["JobId"] = job.Id, ["JobName"] = job.Name, ["Attempt"] = job.Attempt, ["Run"] = job.Run,
        });
        // One span per attempt; a root span, since enqueuing does not carry a trace context.
        using var activity = BackgroundJobsTelemetry.Source.StartActivity($"job {job.Name}", ActivityKind.Consumer, parentContext: default);
        activity?.SetTag("job.id", job.Id.ToString());
        activity?.SetTag("job.name", job.Name);
        activity?.SetTag("job.attempt", job.Attempt);
        activity?.SetTag("job.max_attempts", job.MaxAttempts);
        activity?.SetTag("job.run", job.Run);

        var started = time.GetTimestamp();
        var error = await InvokeAsync(job, activity);
        var success = error == null;
        metrics.AttemptFinished(job.Name, success, time.GetElapsedTime(started));
        jobBreaker.OnFinished(job.Name, success);
        activity?.SetTag("job.outcome", success ? "succeeded" : "failed");
        if (!success) activity?.SetStatus(ActivityStatusCode.Error, error);

        var finishedAt = time.UtcNow();
        try
        {
            var retryAt = error == null ? null : RetryAt(job, finishedAt);
            var transition = error == null ? JobTransition.Succeed : JobTransition.Fail(retry: retryAt != null);
            var outcome = error == null ? AttemptOutcome.Succeeded : AttemptOutcome.Failed;
            var recorded = await store.FinishAsync(job, options.Value.NodeId, transition, outcome, error, finishedAt, retryAt, CancellationToken.None);
            if (recorded) signals.Entered(transition.To);
            else logger.LogWarning("job {JobId} was taken away from this node before it finished", job.Id);
        }
        catch (Exception e)
        {
            // The job stays processing; once its lease expires recovery marks the attempt interrupted.
            logger.LogError(e, "could not record the result of job {JobId}", job.Id);
        }
    }

    private async Task<string?> InvokeAsync(ClaimedJob job, Activity? activity)
    {
        var registration = registry.Find(job.Name);
        if (registration == null) return "unknown job";
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            using var args = JsonDocument.Parse(job.ArgsJson);
            await registration.Invoke(scope.ServiceProvider, args.RootElement, job.ArgsVersion, new JobContext(job.Id, job.Attempt));
            return null;
        }
        catch (Exception e)
        {
            activity?.AddException(e);
            // The stack trace goes in a scope: structured sinks keep it, the console shows one line.
            using (logger.BeginScope(new Dictionary<string, object> { ["Exception"] = e.ToString() }))
                logger.LogInformation("job {JobId} attempt {Attempt} failed: {Error}", job.Id, job.Attempt, e.Message);
            return string.IsNullOrEmpty(e.Message) ? e.GetType().Name : e.Message;
        }
    }

    private static DateTime? RetryAt(ClaimedJob job, DateTime finishedAt) =>
        job.Attempt >= job.MaxAttempts
            ? null
            : finishedAt.AddMilliseconds(job.RetryDelayMs + (job.RetryJitterMs > 0 ? Random.Shared.Next(job.RetryJitterMs + 1) : 0));
}
