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
    IServiceScopeFactory scopes,
    IOptions<BackgroundJobsOptions> options,
    TimeProvider time,
    ILogger<JobExecutor> logger)
{
    public async Task ExecuteAsync(ClaimedJob job)
    {
        var error = await InvokeAsync(job);
        jobBreaker.OnFinished(job.Name, error == null);

        var finishedAt = time.UtcNow();
        try
        {
            var recorded = error == null
                ? await store.SucceedAsync(job, options.Value.NodeId, finishedAt, CancellationToken.None)
                : await store.FailAsync(job, options.Value.NodeId, error, finishedAt, RetryAt(job, finishedAt), CancellationToken.None);
            if (!recorded) logger.LogWarning("job {JobId} was taken away from this node before it finished", job.Id);
        }
        catch (Exception e)
        {
            // The job stays processing; once its lease expires recovery marks the attempt interrupted.
            logger.LogError(e, "could not record the result of job {JobId}", job.Id);
        }
    }

    private async Task<string?> InvokeAsync(ClaimedJob job)
    {
        var registration = registry.Find(job.Name);
        if (registration == null) return "unknown job";
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            using var args = JsonDocument.Parse(job.ArgsJson);
            await registration.Invoke(scope.ServiceProvider, args.RootElement, new JobContext(job.Id, job.Attempt));
            return null;
        }
        catch (Exception e)
        {
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
