using System.Text.Json;
using BackgroundJobs.Storage;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

internal sealed class RecurringJobClient(
    IJobStore store, JobRegistry registry, IJobSignals signals, IOptions<BackgroundJobsOptions> options, TimeProvider time) : IRecurringJobClient
{
    public async Task<RecurringJob> UpsertAsync<TArgs>(
        string id, string jobName, string cron, TArgs args, RecurringJobOptions? jobOptions = null, CancellationToken ct = default)
    {
        RecurringJobIds.Check(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(jobName);
        ArgumentNullException.ThrowIfNull(cron);
        var o = options.Value;
        CronExpression expression;
        try { expression = CronExpression.Parse(cron); }
        catch (FormatException e) { throw new ArgumentException(e.Message, nameof(cron), e); }
        var timeZone = jobOptions?.TimeZone ?? TimeZoneInfo.Utc;
        var maxAttempts = jobOptions?.MaxAttempts ?? o.DefaultMaxAttempts;
        var retryDelay = jobOptions?.RetryDelay ?? o.DefaultRetryDelay;
        var jitter = jobOptions?.RetryJitter ?? o.DefaultRetryJitter;
        if (maxAttempts < 1) throw new ArgumentException("maxAttempts must be >= 1");
        if (retryDelay < TimeSpan.Zero || retryDelay.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryDelay is out of range");
        if (jitter < TimeSpan.Zero || jitter.TotalMilliseconds > int.MaxValue) throw new ArgumentException("retryJitter is out of range");
        var queue = JobQueues.CheckName(jobOptions?.Queue ?? JobQueues.Default);
        var priority = JobQueues.CheckPriority(jobOptions?.Priority ?? JobQueues.DefaultPriority);

        var registration = registry.Find(jobName);
        registration?.CheckArgs(args);

        var now = time.UtcNow();
        var next = expression.GetNextOccurrence(now, timeZone)
                   ?? throw new ArgumentException($"cron '{cron}' never fires", nameof(cron));
        var stored = await store.UpsertRecurringAsync(new RecurringDefinition(
            id, jobName, expression.Expression, timeZone.Id, JsonSerializer.Serialize(args, JobJson.Options),
            registration?.Options.ArgsVersion ?? 1, maxAttempts, (int)retryDelay.TotalMilliseconds, (int)jitter.TotalMilliseconds,
            queue, priority, next, now), ct);
        signals.SchedulesChanged();
        return stored;
    }

    public async Task<RecurringJob> GetAsync(string id, CancellationToken ct = default) =>
        await store.GetRecurringAsync(id, ct) ?? throw new RecurringJobNotFoundException(id);

    public Task<IReadOnlyList<RecurringJob>> ListAsync(CancellationToken ct = default) => store.ListRecurringAsync(ct);

    public async Task RemoveAsync(string id, CancellationToken ct = default)
    {
        if (!await store.RemoveRecurringAsync(id, ct)) throw new RecurringJobNotFoundException(id);
    }

    public async Task<Guid> TriggerAsync(string id, CancellationToken ct = default)
    {
        var definition = await GetAsync(id, ct);
        var job = Occurrences.NewJob(definition, time.UtcNow());
        if (!await store.FireRecurringAsync(new RecurringFire(id, null, null, job), ct)) throw new RecurringJobNotFoundException(id);
        signals.Entered(job.Creation.To);
        return job.Id;
    }
}

internal static class Occurrences
{
    /// The job one occurrence of <paramref name="definition"/> enqueues.
    public static NewJob NewJob(RecurringJob definition, DateTime now) => new(
        Guid.CreateVersion7(now), definition.JobName, definition.Args.GetRawText(), definition.ArgsVersion, definition.MaxAttempts,
        (int)definition.RetryDelay.TotalMilliseconds, (int)definition.RetryJitter.TotalMilliseconds, null, now, definition.Queue, definition.Priority);
}
