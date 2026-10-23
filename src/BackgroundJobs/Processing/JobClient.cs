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
        var o = options.Value;
        var maxAttempts = jobOptions?.MaxAttempts ?? o.DefaultMaxAttempts;
        var retryDelay = jobOptions?.RetryDelay ?? o.DefaultRetryDelay;
        var jitter = jobOptions?.RetryJitter ?? o.DefaultRetryJitter;
        if (maxAttempts < 1) throw new ArgumentException("maxAttempts must be >= 1");
        if (retryDelay < TimeSpan.Zero || retryDelay.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryDelay is out of range");
        if (jitter < TimeSpan.Zero || jitter.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryJitter is out of range");

        // Unknown names are accepted (they fail when executed) and get version 1.
        var registration = registry.Find(jobName);
        registration?.CheckArgs(args);

        var now = time.UtcNow();
        var id = Guid.CreateVersion7(now);
        var job = new NewJob(
            id, jobName, JsonSerializer.Serialize(args, JobJson.Options), registration?.Options.ArgsVersion ?? 1, maxAttempts,
            (int)retryDelay.TotalMilliseconds, (int)jitter.TotalMilliseconds, jobOptions?.RunAt?.UtcDateTime, now);
        await store.InsertAsync(job, ct);
        signals.Entered(job.Creation.To);
        return id;
    }

    public async Task<Job> GetAsync(Guid id, CancellationToken ct = default) =>
        await store.GetAsync(id, ct) ?? throw new JobNotFoundException(id);

    public Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, CancellationToken ct = default)
    {
        if (limit is < 1 or > MaxPageSize) throw new ArgumentException($"limit must be between 1 and {MaxPageSize}");
        return store.ListAsync(after, limit, ct);
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
