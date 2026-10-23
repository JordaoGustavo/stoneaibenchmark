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

    /// Definitions already reported as unusable on this node, so a stuck one is logged once, not every poll.
    private readonly HashSet<string> _reported = [];

    public async Task RunAsync(CancellationToken ct)
    {
        var o = options.Value;
        while (!ct.IsCancellationRequested)
        {
            var promoted = await storeBreaker.TryAsync("promote", () => store.PromoteDueAsync(time.UtcNow(), ct), ct);
            if (promoted > 0) signals.JobsReady();
            // Null on a store failure; true when a due definition could not be fired (and stays due).
            var stuck = await storeBreaker.TryAsync("fire-recurring", async () => Tuple.Create(await FireDueRecurringAsync(ct)), ct);
            // Boxed so a store failure (null) is distinguishable from "nothing scheduled".
            var next = await storeBreaker.TryAsync("next-run-at", async () =>
                Tuple.Create(Earliest(await store.NextRunAtAsync(ct), await store.NextRecurringAtAsync(ct))), ct);
            var wait = next is null || stuck is null ? o.PollInterval
                : next.Item1 is { } at ? Min(at - time.UtcNow(), o.SchedulerMaxWait)
                : o.SchedulerMaxWait;
            if (stuck?.Item1 == true && wait < o.PollInterval) wait = o.PollInterval;
            if (wait <= TimeSpan.Zero) continue;
            try { await signals.Scheduler.WaitAsync(wait, time, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    /// Fires every due definition once, whatever number of its occurrences were missed, and moves it to its next
    /// occurrence after now. Returns whether some definition could not be fired.
    private async Task<bool> FireDueRecurringAsync(CancellationToken ct)
    {
        var stuck = false;
        while (true)
        {
            var now = time.UtcNow();
            var due = await store.DueRecurringAsync(now, RecurringBatch, ct);
            var fired = 0;
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
                    if (_reported.Add(definition.Id)) logger.LogError(e, "recurring job {Id} cannot be scheduled on this node", definition.Id);
                    stuck = true;
                    continue;
                }
                var job = Occurrences.NewJob(definition, now);
                if (await store.FireRecurringAsync(new RecurringFire(definition.Id, definition.NextRunAt, next, job), ct))
                {
                    fired++;
                    signals.Entered(job.Creation.To);
                }
            }
            if (due.Count < RecurringBatch || fired == 0) return stuck;
        }
    }

    private static DateTime? Earliest(DateTime? a, DateTime? b) => a is null ? b : b is null ? a : a < b ? a : b;

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
