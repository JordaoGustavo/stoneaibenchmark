using System.Text.Json;
using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

internal sealed class JobClient(
    IJobStore store, JobRegistry registry, IJobSignals signals, CancelRequests cancels, IOptions<BackgroundJobsOptions> options, TimeProvider time) : IJobClient
{
    public const int MaxPageSize = 1000;

    public async Task<Guid> EnqueueAsync<TArgs>(string jobName, TArgs args, JobOptions? jobOptions = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobName);
        var o = jobOptions;
        JobOptions.CheckIdempotencyKey(o?.IdempotencyKey);
        if (o?.Condition is { } condition && !Enum.IsDefined(condition))
            throw new ArgumentException($"JobOptions.Condition is {condition}; use ContinuationCondition.OnSucceeded or OnFinished");
        var settings = JobSettings.Resolve(options.Value, o?.MaxAttempts, o?.RetryDelay, o?.RetryJitter, o?.Queue, o?.Priority);
        var backoff = BackoffSettings.Resolve(options.Value, o?.RetryBackoff, o?.RetryBackoffMultiplier, o?.MaxRetryDelay);

        // Unknown names are accepted (they fail when executed) and get version 1.
        var registration = registry.Find(jobName);
        registration?.CheckArgs(args);

        var now = time.UtcNow();
        var id = JobIds.Next(now);
        var job = new NewJob(
            id, jobName, JsonSerializer.Serialize(args, JobJson.Options), registration?.Options.ArgsVersion ?? 1, settings.MaxAttempts,
            settings.RetryDelayMs, settings.RetryJitterMs, jobOptions?.RunAt?.UtcDateTime, now, settings.Queue, settings.Priority, o?.IdempotencyKey)
        {
            ContinueAfter = o?.ContinueAfter,
            Condition = o?.Condition ?? ContinuationCondition.OnSucceeded,
            RetryBackoffMultiplier = backoff.Multiplier, MaxRetryDelayMs = backoff.MaxDelayMs,
        };
        var stored = await store.InsertAsync(job, ct);
        // A continuation's creation state is decided by the store; waking both loops is harmless.
        if (stored == id && job.ContinueAfter != null)
        {
            signals.JobsReady();
            signals.SchedulesChanged();
        }
        else if (stored == id) signals.Entered(job.Creation.To);
        return stored;
    }

    public async Task<Job> GetAsync(Guid id, CancellationToken ct = default) =>
        await store.GetAsync(id, ct) ?? throw new JobNotFoundException(id);

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, bool descending = false, CancellationToken ct = default)
    {
        if (limit is < 1 or > MaxPageSize) throw new ArgumentException($"limit is {limit}, but it must be from 1 to {MaxPageSize}; page through more with after", nameof(limit));
        return store.ListAsync(after, limit, descending, ct);
    }

    public Task DeleteAsync(Guid id, CancellationToken ct = default) => ApplyAsync(id, JobTransition.Delete, ct);

    public Task RequeueAsync(Guid id, CancellationToken ct = default) => ApplyAsync(id, JobTransition.Requeue, ct);

    public async Task CancelAsync(Guid id, CancellationToken ct = default)
    {
        // A no-op unless the job runs in this process; other nodes learn of the request from their heartbeat.
        if (await store.CancelAsync(id, time.UtcNow(), ct)) cancels.Request(id);
    }

    private async Task ApplyAsync(Guid id, JobTransition transition, CancellationToken ct)
    {
        await store.ApplyAsync(id, transition, time.UtcNow(), ct);
        signals.Entered(transition.To);
    }
}
