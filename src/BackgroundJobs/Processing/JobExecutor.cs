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
    /// <paramref name="stopping"/> fires when the server stops; the job sees it, merged with its timeout, as its token.
    public async Task ExecuteAsync(ClaimedJob job, CancellationToken stopping)
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
        var (outcome, error) = await InvokeAsync(job, activity, stopping);
        var success = outcome == AttemptOutcome.Succeeded;
        metrics.AttemptFinished(job.Name, outcome, time.GetElapsedTime(started));
        // A shutdown says nothing about the job's health.
        if (outcome != AttemptOutcome.Interrupted) jobBreaker.OnFinished(job.Name, success);
        activity?.SetTag("job.outcome", outcome.ToWire());
        if (outcome == AttemptOutcome.Failed) activity?.SetStatus(ActivityStatusCode.Error, error);

        var finishedAt = time.UtcNow();
        try
        {
            var retryAt = outcome == AttemptOutcome.Failed ? RetryAt(job, finishedAt) : null;
            var transition = outcome switch
            {
                AttemptOutcome.Succeeded => JobTransition.Succeed,
                AttemptOutcome.Interrupted => JobTransition.Interrupt,
                _ => JobTransition.Fail(retry: retryAt != null),
            };
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

    private async Task<(AttemptOutcome Outcome, string? Error)> InvokeAsync(ClaimedJob job, Activity? activity, CancellationToken stopping)
    {
        var registration = registry.Find(job.Name);
        if (registration == null) return (AttemptOutcome.Failed, "unknown job");
        var o = options.Value;
        var timeout = registration.Options.Timeout ?? o.DefaultJobTimeout;
        // Owned by the run, not by this method: an abandoned job keeps its token, and still sees a later shutdown.
        var timedOut = new CancellationTokenSource(timeout, time);
        var cancel = CancellationTokenSource.CreateLinkedTokenSource(stopping, timedOut.Token);

        // On its own thread, so a job that blocks synchronously cannot hold up the timeout below.
        var run = Task.Run(async () =>
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                using var args = JsonDocument.Parse(job.ArgsJson);
                await registration.Invoke(scope.ServiceProvider, args.RootElement, job.ArgsVersion, new JobContext(job.Id, job.Attempt, cancel.Token));
            }
            finally
            {
                cancel.Dispose();
                timedOut.Dispose();
            }
        }, CancellationToken.None);

        if (timeout != Timeout.InfiniteTimeSpan)
        {
            // A job that ignores its token is given up on, so it cannot keep its worker forever. Shutdown alone never
            // abandons a job: stopping waits for running jobs.
            using var finished = new CancellationTokenSource();
            var deadline = Task.Delay(timeout + o.TimeoutGracePeriod, time, finished.Token);
            if (await Task.WhenAny(run, deadline) != run)
            {
                _ = run.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                logger.LogWarning("job {JobId} attempt {Attempt} ignored its timeout; abandoned while it may still be running", job.Id, job.Attempt);
                return (AttemptOutcome.Failed, $"timed out after {Ms(timeout)} ms and did not stop within {Ms(o.TimeoutGracePeriod)} ms");
            }
            await finished.CancelAsync();
        }

        try
        {
            await run;
            return (AttemptOutcome.Succeeded, null);
        }
        // Classified by which token fired, not by exception type: a job may wrap or replace the cancellation exception.
        // The sources may be disposed by now; IsCancellationRequested still answers.
        catch (Exception) when (timedOut.IsCancellationRequested)
        {
            logger.LogInformation("job {JobId} attempt {Attempt} timed out", job.Id, job.Attempt);
            return (AttemptOutcome.Failed, $"timed out after {Ms(timeout)} ms");
        }
        catch (Exception) when (stopping.IsCancellationRequested)
        {
            logger.LogInformation("job {JobId} attempt {Attempt} stopped by shutdown", job.Id, job.Attempt);
            return (AttemptOutcome.Interrupted, null);
        }
        catch (Exception e)
        {
            activity?.AddException(e);
            // The stack trace goes in a scope: structured sinks keep it, the console shows one line.
            using (logger.BeginScope(new Dictionary<string, object> { ["Exception"] = e.ToString() }))
                logger.LogInformation("job {JobId} attempt {Attempt} failed: {Error}", job.Id, job.Attempt, e.Message);
            return (AttemptOutcome.Failed, string.IsNullOrEmpty(e.Message) ? e.GetType().Name : e.Message);
        }
    }

    private static long Ms(TimeSpan t) => (long)t.TotalMilliseconds;

    private static DateTime? RetryAt(ClaimedJob job, DateTime finishedAt) =>
        job.Attempt >= job.MaxAttempts
            ? null
            : finishedAt.AddMilliseconds(job.RetryDelayMs + (job.RetryJitterMs > 0 ? Random.Shared.Next(job.RetryJitterMs + 1) : 0));
}
