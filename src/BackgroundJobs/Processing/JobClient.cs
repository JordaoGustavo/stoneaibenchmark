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
        var job = Prepare(jobName, args, typeof(TArgs), jobOptions, time.UtcNow());
        var stored = await store.InsertAsync(job, ct);
        if (stored == job.Id) Wake(job);
        return stored;
    }

    public async Task<IReadOnlyList<Guid>> EnqueueManyAsync(IEnumerable<JobToEnqueue> jobs, CancellationToken ct = default)
    {
        var batch = PrepareMany(jobs);
        if (batch.Count == 0) return [];
        var stored = await store.InsertManyAsync(batch, ct);
        for (var i = 0; i < batch.Count; i++)
            if (stored[i] == batch[i].Id) Wake(batch[i]);
        return stored;
    }

    /// <summary>
    /// Wakes the local loops for a job just written. A continuation's creation state is decided by the store (awaiting,
    /// or decided at once when its parent already finished), so both loops are woken; that is harmless.
    /// </summary>
    private void Wake(NewJob job)
    {
        if (job.ContinueAfter == null)
        {
            signals.Entered(job.Creation.To);
            return;
        }
        signals.JobsReady();
        signals.SchedulesChanged();
    }

    /// <summary>
    /// Validates and resolves every job before anything is written, so one invalid job fails the whole batch.
    /// </summary>
    /// <exception cref="ArgumentException"/>
    internal IReadOnlyList<NewJob> PrepareMany(IEnumerable<JobToEnqueue> jobs)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        var now = time.UtcNow();
        var batch = new List<NewJob>();
        foreach (var j in jobs)
        {
            try
            {
                if (j == null) throw new ArgumentException("the job is null");
                batch.Add(Prepare(j.JobName, j.Args, j.Args?.GetType() ?? typeof(object), j.Options, now));
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException($"jobs[{batch.Count}]: {e.Message}; nothing was enqueued", nameof(jobs), e);
            }
        }
        return batch;
    }

    /// <summary>
    /// The store; store packages check it is theirs before writing on a caller's transaction.
    /// </summary>
    internal IJobStore Store => store;

    private NewJob Prepare(string jobName, object? args, Type argsType, JobOptions? o, DateTime now)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobName);
        JobOptions.CheckIdempotencyKey(o?.IdempotencyKey);
        if (o?.Condition is { } condition && !Enum.IsDefined(condition))
            throw new ArgumentException($"JobOptions.Condition is {condition}; use ContinuationCondition.OnSucceeded or OnFinished");
        var settings = JobSettings.Resolve(options.Value, o?.MaxAttempts, o?.RetryDelay, o?.RetryJitter, o?.Queue, o?.Priority);
        var backoff = BackoffSettings.Resolve(options.Value, o?.RetryBackoff, o?.RetryBackoffMultiplier, o?.MaxRetryDelay);

        // Unknown names are accepted (they fail when executed) and get version 1.
        var registration = registry.Find(jobName);
        registration?.CheckArgs(args);

        return new NewJob(
            JobIds.Next(now), jobName, JsonSerializer.Serialize(args, argsType, JobJson.Options), registration?.Options.ArgsVersion ?? 1,
            settings.MaxAttempts, settings.RetryDelayMs, settings.RetryJitterMs, o?.RunAt?.UtcDateTime, now, settings.Queue, settings.Priority,
            o?.IdempotencyKey)
        {
            ContinueAfter = o?.ContinueAfter,
            Condition = o?.Condition ?? ContinuationCondition.OnSucceeded,
            RetryBackoffMultiplier = backoff.Multiplier, MaxRetryDelayMs = backoff.MaxDelayMs,
        };
    }

    public async Task<Job> GetAsync(Guid id, CancellationToken ct = default) =>
        await store.GetAsync(id, ct) ?? throw new JobNotFoundException(id);

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, bool descending = false, CancellationToken ct = default)
    {
        if (limit is < 1 or > MaxPageSize) throw new ArgumentException($"limit is {limit}, but it must be from 1 to {MaxPageSize}; page through more with after", nameof(limit));
        return store.ListAsync(after, limit, descending, ct);
    }

    public Task<IReadOnlyList<JobSummary>> ListContinuationsAsync(Guid parentId, Guid? after = null, int limit = 100, CancellationToken ct = default)
    {
        if (limit is < 1 or > MaxPageSize) throw new ArgumentException($"limit is {limit}, but it must be from 1 to {MaxPageSize}; page through more with after", nameof(limit));
        return store.ListContinuationsAsync(parentId, after, limit, ct);
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
