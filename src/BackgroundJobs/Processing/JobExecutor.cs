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
    CancelRequests cancels,
    IServiceScopeFactory scopes,
    IOptions<BackgroundJobsOptions> options,
    TimeProvider time,
    ILogger<JobExecutor> logger)
{
    /// <paramref name="stopping"/> fires when the server stops; the job sees it, merged with its timeout and a user cancel,
    /// as its token. <paramref name="abandon"/> frees the job's worker while it keeps running; false when refused.
    public async Task ExecuteAsync(ClaimedJob job, CancellationToken stopping, Func<bool> abandon)
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
        var (outcome, error) = await InvokeAsync(job, activity, stopping, abandon);
        var success = outcome == AttemptOutcome.Succeeded;
        metrics.AttemptFinished(job.Name, outcome, time.GetElapsedTime(started));
        // A shutdown or a cancel says nothing about the job's health.
        if (outcome is AttemptOutcome.Succeeded or AttemptOutcome.Failed) jobBreaker.OnFinished(job.Name, success);
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
                AttemptOutcome.Cancelled => JobTransition.Cancel,
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

    private async Task<(AttemptOutcome Outcome, string? Error)> InvokeAsync(
        ClaimedJob job, Activity? activity, CancellationToken stopping, Func<bool> abandon)
    {
        var registration = registry.Find(job.Name);
        if (registration == null) return (AttemptOutcome.Failed, "unknown job");
        var o = options.Value;
        var timeout = registration.Options.Timeout ?? o.DefaultJobTimeout;
        // Owned by the run, not by this method: a job given up on at shutdown keeps its token until its code returns.
        var cancelled = cancels.Register(job.Id);
        var timedOut = new CancellationTokenSource(timeout, time);
        var stopAsked = CancellationTokenSource.CreateLinkedTokenSource(timedOut.Token, cancelled.Token);
        var token = CancellationTokenSource.CreateLinkedTokenSource(stopping, stopAsked.Token);

        // Started before the run, which disposes the sources once it returns.
        using var finished = new CancellationTokenSource();
        // A job that ignores a timeout or a cancel gives its worker back after the grace period, or retries until allowed.
        var abandonAt = DelayAfterAsync(stopAsked.Token, o.TimeoutGracePeriod, finished.Token);
        var giveUpAt = DelayAfterAsync(stopping, o.ShutdownTimeout, finished.Token);

        // On its own thread, so a job that blocks synchronously cannot hold up the waits below.
        var run = Task.Run(async () =>
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                using var args = JsonDocument.Parse(job.ArgsJson);
                await registration.Invoke(scope.ServiceProvider, args.RootElement, job.ArgsVersion, new JobContext(job.Id, job.Attempt, token.Token));
            }
            finally
            {
                cancels.Unregister(job.Id, cancelled);
                foreach (var source in new[] { token, stopAsked, timedOut, cancelled }) source.Dispose();
            }
        }, CancellationToken.None);

        try
        {
            while (await Task.WhenAny(run, abandonAt, giveUpAt) is var done && done != run)
            {
                if (done == giveUpAt)
                {
                    // Recorded now so another node can run it; the code dies with this process.
                    _ = run.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                    logger.LogWarning("job {JobId} attempt {Attempt} still running {Timeout} ms after shutdown; given up on", job.Id, job.Attempt, Ms(o.ShutdownTimeout));
                    return cancelled.IsCancellationRequested
                        ? (AttemptOutcome.Cancelled, null)
                        : (AttemptOutcome.Interrupted, $"still running {Ms(o.ShutdownTimeout)} ms after the server began stopping");
                }
                if (abandon())
                {
                    logger.LogWarning("job {JobId} attempt {Attempt} ignored its cancellation; its worker was freed while it keeps running", job.Id, job.Attempt);
                    abandonAt = Task.Delay(Timeout.InfiniteTimeSpan, time, finished.Token);
                }
                else abandonAt = Task.Delay(o.TimeoutGracePeriod, time, finished.Token);
            }
        }
        finally
        {
            await finished.CancelAsync();
        }

        try
        {
            await run;
            return (AttemptOutcome.Succeeded, null);
        }
        // Classified by which token fired, not by exception type: a job may wrap or replace the cancellation exception.
        // The sources are disposed by now; IsCancellationRequested still answers.
        catch (Exception) when (cancelled.IsCancellationRequested)
        {
            logger.LogInformation("job {JobId} attempt {Attempt} cancelled by a user", job.Id, job.Attempt);
            return (AttemptOutcome.Cancelled, null);
        }
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

    /// Completes <paramref name="delay"/> after <paramref name="trigger"/> fires; never, if it does not. Cancelled by <paramref name="abort"/>.
    private async Task DelayAfterAsync(CancellationToken trigger, TimeSpan delay, CancellationToken abort)
    {
        using var either = CancellationTokenSource.CreateLinkedTokenSource(trigger, abort);
        try { await Task.Delay(Timeout.InfiniteTimeSpan, time, either.Token); }
        catch (OperationCanceledException) when (!abort.IsCancellationRequested) { }
        await Task.Delay(delay, time, abort);
    }

    private static long Ms(TimeSpan t) => (long)t.TotalMilliseconds;

    private static DateTime? RetryAt(ClaimedJob job, DateTime finishedAt) =>
        job.Attempt >= job.MaxAttempts
            ? null
            : finishedAt.AddMilliseconds(job.RetryDelayMs + (job.RetryJitterMs > 0 ? Random.Shared.Next(job.RetryJitterMs + 1) : 0));
}
