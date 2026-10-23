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
        CronExpression expression;
        try { expression = CronExpression.Parse(cron); }
        catch (FormatException e) { throw new ArgumentException(e.Message, nameof(cron), e); }
        var timeZone = jobOptions?.TimeZone ?? TimeZoneInfo.Utc;
        var o = jobOptions;
        var settings = JobSettings.Resolve(options.Value, o?.MaxAttempts, o?.RetryDelay, o?.RetryJitter, o?.Queue, o?.Priority);

        // Unlike a one-off enqueue, a typo here would fail at every occurrence, so the name must be registered.
        var registration = registry.Find(jobName) ?? throw new ArgumentException($"job '{jobName}' is not registered", nameof(jobName));
        registration.CheckArgs(args);

        var now = time.UtcNow();
        var next = expression.GetNextOccurrence(now, timeZone)
                   ?? throw new ArgumentException($"cron '{cron}' never fires", nameof(cron));
        var stored = await store.UpsertRecurringAsync(new RecurringDefinition(
            id, jobName, expression.Expression, timeZone.Id, JsonSerializer.Serialize(args, JobJson.Options),
            registration.Options.ArgsVersion, settings.MaxAttempts, settings.RetryDelayMs, settings.RetryJitterMs,
            settings.Queue, settings.Priority, next, now), ct);
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
        // Retried when an upsert lands between the read and the fire, so the job carries the definition as it is now.
        while (true)
        {
            var definition = await GetAsync(id, ct);
            var job = definition.NewJob(time.UtcNow());
            switch (await store.FireRecurringAsync(new RecurringFire(definition, null, null, job), ct))
            {
                case RecurringFireResult.Fired:
                    signals.Entered(job.Creation.To);
                    return job.Id;
                case RecurringFireResult.Gone:
                    throw new RecurringJobNotFoundException(id);
            }
        }
    }
}
