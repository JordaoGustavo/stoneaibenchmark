using BackgroundJobs.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BackgroundJobs.Processing;

/// Promotes due scheduled jobs (new schedules and retries) to enqueued and fires due recurring definitions. Sleeps until
/// the next run_at or recurring occurrence (capped by SchedulerMaxWait) and wakes early when either changes.
internal sealed class Scheduler(
    IJobStore store, StoreCircuitBreaker storeBreaker, Signals signals, IOptions<BackgroundJobsOptions> options, TimeProvider time,
    ILogger<Scheduler> logger)
{
    private const int RecurringBatch = 100;

    /// Definitions this node cannot schedule (a time zone or cron it cannot read); neither changes while the process
    /// runs, so they are left to other nodes and logged once.
    private readonly HashSet<string> _unusable = [];

    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            var promoted = await storeBreaker.TryAsync("promote", () => store.PromoteDueAsync(time.UtcNow(), ct), ct);
            if (promoted > 0) signals.JobsReady();
            await storeBreaker.TryAsync("fire-recurring", () => FireDueRecurringAsync(ct), ct);
            // Boxed so a store failure (null) is distinguishable from "nothing scheduled".
            var next = await storeBreaker.TryAsync("next-run-at", async () =>
                Tuple.Create(Earliest(await store.NextRunAtAsync(ct), await store.NextRecurringAtAsync(_unusable, ct))), ct);
            var wait = next is null ? o.PollInterval
                : next.Item1 is { } at ? Min(at - time.UtcNow(), o.SchedulerMaxWait)
                : o.SchedulerMaxWait;
            if (wait <= TimeSpan.Zero) continue;
            try { await signals.Scheduler.WaitAsync(wait, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    /// Fires every due definition once, whatever number of its occurrences were missed, and moves it to its next
    /// occurrence after now. A definition upserted meanwhile is read again on the next pass.
    private async Task<bool> FireDueRecurringAsync(CancellationToken ct)
    {
        while (true)
        {
            var now = time.UtcNow();
            var due = await store.DueRecurringAsync(now, RecurringBatch, _unusable, ct);
            foreach (var definition in due)
            {
                DateTime next;
                try
                {
                    var timeZone = TimeZoneInfo.FindSystemTimeZoneById(definition.TimeZone);
                    next = CronExpression.Parse(definition.Cron).GetNextOccurrence(now, timeZone) ?? DateTime.MaxValue;
                }
                catch (Exception e) when (e is FormatException or TimeZoneNotFoundException or InvalidTimeZoneException)
                {
                    _unusable.Add(definition.Id);
                    logger.LogError(e, "recurring job {Id} cannot be scheduled on this node", definition.Id);
                    continue;
                }
                var job = definition.NewJob(now);
                if (await store.FireRecurringAsync(new RecurringFire(definition, definition.NextRunAt, next, job), ct) == RecurringFireResult.Fired)
                    signals.Entered(job.Creation.To);
            }
            // Every row of a full batch was fired, found stale or excluded, so the next query moves past it.
            if (due.Count < RecurringBatch) return true;
        }
    }

    private static DateTime? Earliest(DateTime? a, DateTime? b) => a is null ? b : b is null ? a : a < b ? a : b;

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
