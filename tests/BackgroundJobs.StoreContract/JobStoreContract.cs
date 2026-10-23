using BackgroundJobs.Storage;

namespace BackgroundJobs.StoreContract;

/// What IJobStore promises, checked call by call. Every store runs the same tests (one subclass per store), so a
/// behaviour one store has and the other lacks fails here instead of in review.
public abstract class JobStoreContract : IAsyncLifetime
{
    private const string Node = "node-a", OtherNode = "node-b";
    private static readonly CancellationToken Ct = CancellationToken.None;
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    /// Millisecond precision so values survive a round trip through Postgres (microseconds) unchanged.
    private static readonly DateTime T0 = Truncate(DateTime.UtcNow);

    private IJobStore _store = null!;
    private IAsyncDisposable _owner = null!;

    protected abstract Task<(IJobStore Store, IAsyncDisposable Owner)> CreateStoreAsync();

    public async Task InitializeAsync() => (_store, _owner) = await CreateStoreAsync();

    public Task DisposeAsync() => _owner.DisposeAsync().AsTask();

    // ---- insert, get, list ----

    [Fact]
    public async Task Insert_without_run_at_enqueues()
    {
        var id = await Insert();
        var job = await Get(id);
        Assert.Equal(JobState.Enqueued, job.State);
        Assert.Null(job.ScheduledAt);
        Assert.Equal(0, job.Attempts);
        Assert.Equal(T0, job.CreatedAt);
        Assert.Equal([(JobState.Enqueued, Reasons.Created)], History(job));
    }

    [Fact]
    public async Task Insert_with_future_run_at_schedules()
    {
        var id = await Insert(runAt: T0.AddMinutes(5));
        var job = await Get(id);
        Assert.Equal(JobState.Scheduled, job.State);
        Assert.Equal(T0.AddMinutes(5), job.ScheduledAt);
        Assert.Equal([(JobState.Scheduled, Reasons.ScheduledAtCreation)], History(job));
        Assert.Equal(T0.AddMinutes(5), await _store.NextRunAtAsync(Ct));
    }

    [Fact]
    public async Task Get_unknown_job_is_null() => Assert.Null(await _store.GetAsync(Guid.CreateVersion7(), Ct));

    [Fact]
    public async Task Insert_keeps_args_queue_and_priority()
    {
        var id = await Insert(queue: "emails", priority: 7, args: """{"to":"a@b.c"}""");
        var job = await Get(id);
        Assert.Equal("emails", job.Queue);
        Assert.Equal(7, job.Priority);
        Assert.Equal("a@b.c", job.Args.GetProperty("to").GetString());
        Assert.Equal(3, job.MaxAttempts);
    }

    [Fact]
    public async Task List_is_ascending_by_id_and_pages_after_a_cursor()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 5; i++) ids.Add(await Insert());
        ids.Sort((a, b) => string.CompareOrdinal(a.ToString(), b.ToString()));

        var first = await _store.ListAsync(null, 3, false, Ct);
        Assert.Equal(ids.Take(3), first.Select(j => j.Id));
        var rest = await _store.ListAsync(first[^1].Id, 10, false, Ct);
        Assert.Equal(ids.Skip(3), rest.Select(j => j.Id));
    }

    [Fact]
    public async Task List_descending_is_newest_first_and_pages_before_a_cursor()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 5; i++) ids.Add(await Insert());
        ids.Sort((a, b) => string.CompareOrdinal(b.ToString(), a.ToString()));

        var first = await _store.ListAsync(null, 3, true, Ct);
        Assert.Equal(ids.Take(3), first.Select(j => j.Id));
        var rest = await _store.ListAsync(first[^1].Id, 10, true, Ct);
        Assert.Equal(ids.Skip(3), rest.Select(j => j.Id));
    }

    // ---- idempotency keys ----

    [Fact]
    public async Task Insert_with_a_held_key_returns_the_holder_and_inserts_nothing()
    {
        var holder = await Insert(key: "k");
        var again = await Insert(key: "k");
        Assert.Equal(holder, again);
        Assert.Single(await _store.ListAsync(null, 10, false, Ct));
    }

    [Fact]
    public async Task A_finished_job_releases_its_key()
    {
        var first = await Insert(key: "k");
        await _store.ApplyAsync(first, JobTransition.Delete, T0, Ct);
        var second = await Insert(key: "k");
        Assert.NotEqual(first, second);
    }

    [Fact]
    public async Task Concurrent_inserts_with_one_key_create_one_job()
    {
        var ids = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Insert(key: "k")));
        Assert.Single(ids.Distinct());
        Assert.Single(await _store.ListAsync(null, 100, false, Ct));
    }

    [Fact]
    public async Task Requeue_onto_a_held_key_conflicts_and_changes_nothing()
    {
        var old = await Insert(key: "k");
        await _store.ApplyAsync(old, JobTransition.Delete, T0, Ct);
        var holder = await Insert(key: "k");

        var e = await Assert.ThrowsAsync<IdempotencyKeyConflictException>(() => _store.ApplyAsync(old, JobTransition.Requeue, T0, Ct));
        Assert.Equal(holder, e.HolderJobId);
        Assert.Equal(JobState.Deleted, (await Get(old)).State);
    }

    // ---- batch insert ----

    [Fact]
    public async Task InsertMany_creates_every_job_with_its_own_state_and_history()
    {
        var jobs = new[]
        {
            New(queue: "emails", priority: 7, args: """{"n":1}"""),
            New(runAt: T0.AddMinutes(5)),
            New(name: "other", maxAttempts: 5),
        };
        var ids = await _store.InsertManyAsync(jobs, Ct);

        Assert.Equal(jobs.Select(j => j.Id), ids);
        var first = await Get(ids[0]);
        Assert.Equal((JobState.Enqueued, "emails", 7, 1), (first.State, first.Queue, first.Priority, first.Args.GetProperty("n").GetInt32()));
        Assert.Equal([(JobState.Enqueued, Reasons.Created)], History(first));
        var scheduled = await Get(ids[1]);
        Assert.Equal((JobState.Scheduled, T0.AddMinutes(5)), (scheduled.State, scheduled.ScheduledAt));
        Assert.Equal([(JobState.Scheduled, Reasons.ScheduledAtCreation)], History(scheduled));
        Assert.Equal(("other", 5), ((await Get(ids[2])).Name, (await Get(ids[2])).MaxAttempts));
    }

    [Fact]
    public async Task InsertMany_enqueues_in_batch_order()
    {
        var jobs = Enumerable.Range(0, 20).Select(_ => New()).ToList();
        var ids = await _store.InsertManyAsync(jobs, Ct);
        Assert.Equal(ids, (await Claim(limit: 20, order: ClaimOrder.Fifo)).Select(j => j.Id));
    }

    [Fact]
    public async Task InsertMany_returns_the_holders_of_held_keys_including_earlier_jobs_of_the_batch()
    {
        var holder = await Insert(key: "held");
        var jobs = new[] { New(key: "held"), New(key: "new"), New(), New(key: "new") };
        var ids = await _store.InsertManyAsync(jobs, Ct);

        Assert.Equal([holder, jobs[1].Id, jobs[2].Id, jobs[1].Id], ids);
        Assert.Equal(3, (await _store.ListAsync(null, 10, false, Ct)).Count);
        Assert.Null(await _store.GetAsync(jobs[0].Id, Ct));
        Assert.Null(await _store.GetAsync(jobs[3].Id, Ct));
    }

    [Fact]
    public async Task Concurrent_batches_with_one_key_create_one_job()
    {
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => _store.InsertManyAsync([New(), New(key: "k")], Ct)));
        Assert.Single(results.Select(r => r[1]).Distinct());
        Assert.Equal(9, (await _store.ListAsync(null, 100, false, Ct)).Count);
    }

    [Fact]
    public async Task InsertMany_of_nothing_inserts_nothing()
    {
        Assert.Empty(await _store.InsertManyAsync([], Ct));
        Assert.Empty(await _store.ListAsync(null, 10, false, Ct));
    }

    // ---- promote ----

    [Fact]
    public async Task Promote_moves_only_due_jobs()
    {
        var due = await Insert(runAt: T0.AddSeconds(1));
        var later = await Insert(runAt: T0.AddMinutes(1));

        Assert.Equal(1, await _store.PromoteDueAsync(T0.AddSeconds(2), Ct));
        var promoted = await Get(due);
        Assert.Equal(JobState.Enqueued, promoted.State);
        Assert.Null(promoted.ScheduledAt);
        Assert.Equal((JobState.Enqueued, Reasons.BecameDue), History(promoted)[^1]);
        Assert.Equal(JobState.Scheduled, (await Get(later)).State);
        Assert.Equal(T0.AddMinutes(1), await _store.NextRunAtAsync(Ct));
    }

    /// Promoted jobs share enqueued_at; their FIFO order must follow run_at, not insertion or id order.
    [Fact]
    public async Task Promoted_jobs_are_claimed_in_run_at_order()
    {
        // Neither ascending nor descending id order matches run_at order.
        var inserted = new List<(int Second, Guid Id)>();
        foreach (var second in new[] { 3, 1, 5, 2, 4 }) inserted.Add((second, await Insert(runAt: T0.AddSeconds(second))));

        Assert.Equal(5, await _store.PromoteDueAsync(T0.AddSeconds(10), Ct));
        var claimed = await Claim(limit: 5, order: ClaimOrder.Fifo);
        Assert.Equal(inserted.OrderBy(j => j.Second).Select(j => j.Id), claimed.Select(c => c.Id));
    }

    // ---- claim ----

    [Fact]
    public async Task Claim_opens_an_attempt_and_locks_the_job()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        Assert.Equal(id, claimed.Id);
        Assert.Equal(1, claimed.Attempt);
        Assert.Equal(1, claimed.Run);

        var job = await Get(id);
        Assert.Equal(JobState.Processing, job.State);
        Assert.Equal(1, job.Attempts);
        Assert.Equal((JobState.Processing, Reasons.Claimed), History(job)[^1]);
        var attempt = Assert.Single(job.AttemptHistory);
        Assert.Equal(1, attempt.Attempt);
        Assert.Null(attempt.FinishedAt);

        Assert.Empty(await Claim());
    }

    [Fact]
    public async Task Claim_by_priority_then_fifo()
    {
        var a = await Insert(priority: 0);
        var b = await Insert(priority: 5);
        var c = await Insert(priority: 0);
        var d = await Insert(priority: 5);
        Assert.Equal([b, d, a, c], (await Claim(limit: 10)).Select(j => j.Id));
    }

    [Fact]
    public async Task Fifo_claim_ignores_priority()
    {
        var a = await Insert(priority: 0);
        var b = await Insert(priority: 5);
        var c = await Insert(priority: -5);
        Assert.Equal([a, b, c], (await Claim(limit: 10, order: ClaimOrder.Fifo)).Select(j => j.Id));
    }

    [Fact]
    public async Task Claim_respects_limit_queue_and_excluded_names()
    {
        await Insert(queue: "other");
        await Insert(name: "skipped");
        var a = await Insert();
        var b = await Insert();
        await Insert();

        var claimed = await Claim(limit: 2, excluded: ["skipped"]);
        Assert.Equal([a, b], claimed.Select(j => j.Id));
    }

    [Theory]
    [InlineData(ClaimOrder.Priority)]
    [InlineData(ClaimOrder.Fifo)]
    public async Task Claim_takes_only_the_requested_names_and_leaves_the_others_enqueued(ClaimOrder order)
    {
        var foreign = await Insert(name: "foreign", priority: 10);
        var a = await Insert(name: "a");
        var other = await Insert(name: "other");
        var b = await Insert(name: "b");

        var claimed = await Claim(limit: 5, order: order, names: ["a", "b"]);
        Assert.Equal([a, b], claimed.Select(j => j.Id));
        Assert.Equal(JobState.Enqueued, (await Get(foreign)).State);
        Assert.Equal(0, (await Get(foreign)).Attempts);
        Assert.Equal(JobState.Enqueued, (await Get(other)).State);

        Assert.Empty(await Claim(limit: 5, order: order, names: ["a", "b"]));
        Assert.Empty(await Claim(limit: 5, order: order, names: []));
        Assert.Equal(new HashSet<Guid> { foreign, other }, (await Claim(limit: 5, order: order)).Select(j => j.Id).ToHashSet());
    }

    [Fact]
    public async Task Claim_with_names_still_honours_concurrency_limits_and_exclusions()
    {
        var a1 = await Insert(name: "a");
        await Insert(name: "a");
        await Insert(name: "foreign");
        await Insert(name: "skipped");
        var b1 = await Insert(name: "b");
        var b2 = await Insert(name: "b");

        var claimed = await Claim(
            limit: 5, names: ["a", "b", "skipped"], excluded: ["skipped"], limits: new Dictionary<string, int> { ["a"] = 1 });
        Assert.Equal([a1, b1, b2], claimed.Select(j => j.Id));
    }

    [Fact]
    public async Task Concurrent_claims_never_hand_out_a_job_twice()
    {
        for (var i = 0; i < 20; i++) await Insert();
        var batches = await Task.WhenAll(Enumerable.Range(0, 8).Select(n => Claim(limit: 5, node: $"n{n}")));
        var ids = batches.SelectMany(b => b).Select(j => j.Id).ToList();
        Assert.Equal(20, ids.Count);
        Assert.Equal(20, ids.Distinct().Count());
    }

    [Fact]
    public async Task Claim_skips_jobs_over_their_concurrency_limit_and_keeps_filling_with_others()
    {
        var l1 = await Insert(name: "limited");
        await Insert(name: "limited");
        await Insert(name: "limited");
        var f1 = await Insert();
        var f2 = await Insert();

        var claimed = await Claim(limit: 3, limits: new Dictionary<string, int> { ["limited"] = 1 });
        Assert.Equal([l1, f1, f2], claimed.Select(j => j.Id));
    }

    [Fact]
    public async Task Concurrency_limit_counts_jobs_processing_on_any_node_until_they_finish()
    {
        for (var i = 0; i < 3; i++) await Insert(name: "limited");
        var limits = new Dictionary<string, int> { ["limited"] = 2 };

        Assert.Single(await Claim(limits: limits, node: OtherNode));
        var running = Assert.Single(await Claim(limit: 5, limits: limits));
        Assert.Empty(await Claim(limit: 5, limits: limits));

        Assert.True(await Finish(running, JobTransition.Succeed, AttemptOutcome.Succeeded));
        Assert.Single(await Claim(limit: 5, limits: limits));
    }

    [Fact]
    public async Task Concurrency_limit_spans_queues()
    {
        await Insert(name: "limited", queue: "other");
        await Insert(name: "limited");
        var limits = new Dictionary<string, int> { ["limited"] = 1 };

        Assert.Single(await Claim(queue: "other", limits: limits));
        Assert.Empty(await Claim(limits: limits));
    }

    [Fact]
    public async Task Concurrent_claims_never_exceed_a_concurrency_limit()
    {
        for (var i = 0; i < 20; i++) await Insert(name: "limited");
        var limits = new Dictionary<string, int> { ["limited"] = 3 };
        var batches = await Task.WhenAll(Enumerable.Range(0, 8).Select(n => Claim(limit: 5, node: $"n{n}", limits: limits)));
        Assert.Equal(3, batches.Sum(b => b.Count));
    }

    // ---- finish ----

    [Fact]
    public async Task Succeed_by_the_owner_closes_the_attempt()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        Assert.True(await Finish(claimed, JobTransition.Succeed, AttemptOutcome.Succeeded));

        var job = await Get(id);
        Assert.Equal(JobState.Succeeded, job.State);
        Assert.Equal(T0.AddSeconds(1), job.UpdatedAt);
        var attempt = Assert.Single(job.AttemptHistory);
        Assert.Equal(AttemptOutcome.Succeeded, attempt.Outcome);
        Assert.Equal(T0.AddSeconds(1), attempt.FinishedAt);
    }

    [Fact]
    public async Task Finish_by_another_node_or_with_a_stale_version_changes_nothing()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        Assert.False(await Finish(claimed, JobTransition.Succeed, AttemptOutcome.Succeeded, node: OtherNode));
        Assert.False(await Finish(claimed with { Version = claimed.Version - 1 }, JobTransition.Succeed, AttemptOutcome.Succeeded));

        var job = await Get(id);
        Assert.Equal(JobState.Processing, job.State);
        Assert.Null(Assert.Single(job.AttemptHistory).Outcome);
    }

    [Fact]
    public async Task Fail_with_retry_schedules_at_retry_at_and_records_the_error()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        Assert.True(await Finish(claimed, JobTransition.Fail(retry: true), AttemptOutcome.Failed, error: "boom", retryAt: T0.AddMinutes(1)));

        var job = await Get(id);
        Assert.Equal(JobState.Scheduled, job.State);
        Assert.Equal(T0.AddMinutes(1), job.ScheduledAt);
        Assert.Equal((JobState.Scheduled, Reasons.RetryScheduled), History(job)[^1]);
        Assert.Equal("boom", Assert.Single(job.AttemptHistory).Error);

        await _store.PromoteDueAsync(T0.AddMinutes(2), Ct);
        var retry = Assert.Single(await Claim());
        Assert.Equal(2, retry.Attempt);
    }

    [Fact]
    public async Task Interrupt_requeues_and_refunds_the_attempt()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        Assert.True(await Finish(claimed, JobTransition.Interrupt, AttemptOutcome.Interrupted));

        var job = await Get(id);
        Assert.Equal(JobState.Enqueued, job.State);
        Assert.Equal(1, job.RefundedAttempts);
        Assert.Equal((JobState.Enqueued, Reasons.InterruptedByShutdown), History(job)[^1]);

        var again = Assert.Single(await Claim());
        Assert.Equal(2, again.Attempt);
        Assert.Equal(1, again.CountedAttempts);
    }

    [Fact]
    public async Task A_cancel_request_wins_over_failure_but_not_over_success()
    {
        var failing = await Insert();
        var succeeding = await Insert();
        var claimed = await Claim(limit: 2);
        Assert.True(await _store.CancelAsync(failing, T0, Ct));
        Assert.True(await _store.CancelAsync(succeeding, T0, Ct));

        Assert.True(await Finish(claimed.Single(c => c.Id == failing), JobTransition.Fail(retry: true), AttemptOutcome.Failed, retryAt: T0.AddMinutes(1)));
        Assert.True(await Finish(claimed.Single(c => c.Id == succeeding), JobTransition.Succeed, AttemptOutcome.Succeeded));

        var cancelled = await Get(failing);
        Assert.Equal(JobState.Cancelled, cancelled.State);
        Assert.Equal(AttemptOutcome.Cancelled, Assert.Single(cancelled.AttemptHistory).Outcome);
        Assert.Equal(JobState.Succeeded, (await Get(succeeding)).State);
    }

    // ---- leases ----

    [Fact]
    public async Task Renew_extends_only_own_processing_jobs_and_reports_cancel_requests()
    {
        var mine = await Insert();
        var cancelled = await Insert();
        var theirs = await Insert();
        await Claim(limit: 2);
        await Claim(node: OtherNode);
        await _store.CancelAsync(cancelled, T0, Ct);

        var renewal = await _store.RenewLeasesAsync(Node, [mine, cancelled, theirs, Guid.CreateVersion7()], T0, Lease, Ct);
        Assert.Equal(new HashSet<Guid> { mine, cancelled }, renewal.Renewed.ToHashSet());
        Assert.Equal([cancelled], renewal.CancelRequested);
    }

    [Fact]
    public async Task Recover_leaves_jobs_whose_lease_has_not_expired()
    {
        var id = await Insert();
        await Claim();
        Assert.Equal(0, await _store.RecoverAsync(DateTime.UtcNow, Ct));
        Assert.Equal(JobState.Processing, (await Get(id)).State);
    }

    [Fact]
    public async Task Recover_requeues_an_expired_job_and_interrupts_its_attempt()
    {
        var id = await Insert();
        await ClaimAndExpire();

        Assert.Equal(1, await _store.RecoverAsync(DateTime.UtcNow, Ct));
        var job = await Get(id);
        Assert.Equal(JobState.Enqueued, job.State);
        Assert.Equal((JobState.Enqueued, Reasons.RecoveredAfterCrash), History(job)[^1]);
        Assert.Equal(AttemptOutcome.Interrupted, Assert.Single(job.AttemptHistory).Outcome);
        Assert.Equal(0, job.RefundedAttempts);
    }

    [Fact]
    public async Task Recover_fails_a_job_out_of_attempts()
    {
        var id = await Insert(maxAttempts: 1);
        await ClaimAndExpire();

        Assert.Equal(1, await _store.RecoverAsync(DateTime.UtcNow, Ct));
        var job = await Get(id);
        Assert.Equal(JobState.Failed, job.State);
        Assert.Equal((JobState.Failed, Reasons.MaxAttemptsExhausted), History(job)[^1]);
    }

    [Fact]
    public async Task Recover_cancels_a_job_whose_cancel_was_requested()
    {
        var id = await Insert();
        await ClaimAndExpire(beforeExpiry: () => _store.CancelAsync(id, DateTime.UtcNow, Ct));

        Assert.Equal(1, await _store.RecoverAsync(DateTime.UtcNow, Ct));
        var job = await Get(id);
        Assert.Equal(JobState.Cancelled, job.State);
        Assert.Equal(AttemptOutcome.Cancelled, Assert.Single(job.AttemptHistory).Outcome);
    }

    [Fact]
    public async Task A_recovered_job_can_no_longer_be_finished_by_its_old_owner()
    {
        await Insert();
        var claimed = await ClaimAndExpire();
        await _store.RecoverAsync(DateTime.UtcNow, Ct);
        Assert.False(await Finish(claimed, JobTransition.Succeed, AttemptOutcome.Succeeded));
        Assert.Empty((await _store.RenewLeasesAsync(Node, [claimed.Id], DateTime.UtcNow, Lease, Ct)).Renewed);
    }

    // ---- cancel ----

    [Fact]
    public async Task Cancel_moves_scheduled_and_enqueued_jobs_directly()
    {
        var scheduled = await Insert(runAt: T0.AddMinutes(1));
        var enqueued = await Insert();
        Assert.False(await _store.CancelAsync(scheduled, T0, Ct));
        Assert.False(await _store.CancelAsync(enqueued, T0, Ct));
        foreach (var id in new[] { scheduled, enqueued })
        {
            var job = await Get(id);
            Assert.Equal(JobState.Cancelled, job.State);
            Assert.Equal((JobState.Cancelled, Reasons.CancelledByUser), History(job)[^1]);
        }
        Assert.Null(await _store.NextRunAtAsync(Ct));
    }

    [Fact]
    public async Task Cancel_of_a_processing_job_only_records_the_request()
    {
        var id = await Insert();
        await Claim();
        Assert.True(await _store.CancelAsync(id, T0, Ct));
        Assert.Equal(JobState.Processing, (await Get(id)).State);
    }

    [Fact]
    public async Task Cancel_rejects_finished_and_unknown_jobs()
    {
        var id = await Insert();
        await _store.ApplyAsync(id, JobTransition.Delete, T0, Ct);
        var e = await Assert.ThrowsAsync<InvalidJobTransitionException>(() => _store.CancelAsync(id, T0, Ct));
        Assert.Equal(JobState.Deleted, e.CurrentState);
        await Assert.ThrowsAsync<JobNotFoundException>(() => _store.CancelAsync(Guid.CreateVersion7(), T0, Ct));
    }

    [Fact]
    public async Task Leaving_processing_clears_the_cancel_request()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        await _store.CancelAsync(id, T0, Ct);
        await Finish(claimed, JobTransition.Succeed, AttemptOutcome.Succeeded);
        await _store.ApplyAsync(id, JobTransition.Requeue, T0.AddSeconds(2), Ct);

        var again = Assert.Single(await Claim());
        Assert.Empty((await _store.RenewLeasesAsync(Node, [id], T0, Lease, Ct)).CancelRequested);
        Assert.True(await Finish(again, JobTransition.Fail(retry: false), AttemptOutcome.Failed));
        Assert.Equal(JobState.Failed, (await Get(id)).State);
    }

    // ---- user transitions ----

    [Fact]
    public async Task Delete_then_requeue_starts_a_new_run_keeping_history()
    {
        var id = await Insert();
        var claimed = Assert.Single(await Claim());
        await Finish(claimed, JobTransition.Fail(retry: false), AttemptOutcome.Failed);

        await _store.ApplyAsync(id, JobTransition.Requeue, T0.AddSeconds(2), Ct);
        var job = await Get(id);
        Assert.Equal(JobState.Enqueued, job.State);
        Assert.Equal(0, job.Attempts);
        Assert.Equal(
            [Reasons.Created, Reasons.Claimed, Reasons.MaxAttemptsExhausted, Reasons.RequeuedByUser],
            job.History.Select(h => h.Reason));

        var again = Assert.Single(await Claim());
        Assert.Equal(1, again.Attempt);
        Assert.Equal(2, again.Run);
    }

    [Fact]
    public async Task Apply_rejects_a_transition_from_the_wrong_state_and_unknown_jobs()
    {
        var id = await Insert();
        await Claim();
        var e = await Assert.ThrowsAsync<InvalidJobTransitionException>(() => _store.ApplyAsync(id, JobTransition.Delete, T0, Ct));
        Assert.Equal(JobState.Processing, e.CurrentState);
        await Assert.ThrowsAsync<JobNotFoundException>(() => _store.ApplyAsync(Guid.CreateVersion7(), JobTransition.Delete, T0, Ct));
    }

    /// Concurrent user transitions on one job: exactly one applies, the others see the new state, never a phantom conflict.
    [Fact]
    public async Task Concurrent_deletes_apply_once()
    {
        var id = await Insert();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            try { await _store.ApplyAsync(id, JobTransition.Delete, T0, Ct); return null; }
            catch (InvalidJobTransitionException e) { return (JobState?)e.CurrentState; }
        }));
        Assert.Single(results, r => r == null);
        Assert.All(results.Where(r => r != null), r => Assert.Equal(JobState.Deleted, r));
        Assert.Single((await Get(id)).History, h => h.State == JobState.Deleted);
    }

    // ---- queues and consumers ----

    [Fact]
    public async Task Queue_stats_count_ready_jobs_and_live_consumers()
    {
        await Insert(queue: "a");
        await Insert(queue: "a");
        await Insert(queue: "b");
        await Insert(queue: "c", runAt: T0.AddMinutes(1));
        await _store.RegisterConsumerAsync(Node, ["a"], T0, T0.AddSeconds(30), Ct);

        var stats = (await _store.QueueStatsAsync(T0.AddSeconds(1), Ct)).ToDictionary(s => s.Queue);
        Assert.Equal(["a", "b"], stats.Keys.Order());
        Assert.Equal(2, stats["a"].Ready);
        Assert.Equal(T0, stats["a"].OldestReadyAt);
        Assert.True(stats["a"].HasConsumer);
        Assert.False(stats["b"].HasConsumer);

        Assert.False((await _store.QueueStatsAsync(T0.AddMinutes(1), Ct)).Single(s => s.Queue == "a").HasConsumer);
    }

    [Fact]
    public async Task Registering_replaces_the_nodes_queues_and_empty_deregisters()
    {
        await Insert(queue: "a");
        await Insert(queue: "b");
        await _store.RegisterConsumerAsync(Node, ["a"], T0, T0.AddSeconds(30), Ct);
        await _store.RegisterConsumerAsync(Node, ["b"], T0, T0.AddSeconds(30), Ct);
        var stats = (await _store.QueueStatsAsync(T0, Ct)).ToDictionary(s => s.Queue, s => s.HasConsumer);
        Assert.False(stats["a"]);
        Assert.True(stats["b"]);

        await _store.RegisterConsumerAsync(Node, [], T0, T0.AddSeconds(30), Ct);
        Assert.All(await _store.QueueStatsAsync(T0, Ct), s => Assert.False(s.HasConsumer));
    }

    // ---- retention ----

    [Fact]
    public async Task Purge_removes_only_old_finished_jobs_up_to_the_limit()
    {
        var old1 = await Insert();
        var old2 = await Insert();
        var recent = await Insert();
        var pending = await Insert();
        await _store.ApplyAsync(old1, JobTransition.Delete, T0.AddSeconds(1), Ct);
        await _store.ApplyAsync(old2, JobTransition.Delete, T0.AddSeconds(2), Ct);
        await _store.ApplyAsync(recent, JobTransition.Delete, T0.AddMinutes(10), Ct);

        Assert.Equal(1, await _store.PurgeFinishedAsync(T0.AddMinutes(1), 1, Ct));
        Assert.Null(await _store.GetAsync(old1, Ct));
        Assert.Equal(1, await _store.PurgeFinishedAsync(T0.AddMinutes(1), 10, Ct));
        Assert.Null(await _store.GetAsync(old2, Ct));
        Assert.NotNull(await _store.GetAsync(recent, Ct));
        Assert.NotNull(await _store.GetAsync(pending, Ct));
    }

    // ---- recurring ----

    [Fact]
    public async Task Upsert_keeps_next_run_when_the_schedule_is_unchanged()
    {
        var created = await Upsert("r1", "*/5 * * * *", T0.AddMinutes(5), now: T0);
        Assert.Equal(T0.AddMinutes(5), created.NextRunAt);
        Assert.Equal(T0, created.CreatedAt);

        var sameCron = await Upsert("r1", "*/5 * * * *", T0.AddMinutes(9), now: T0.AddMinutes(1), args: """{"v":2}""");
        Assert.Equal(T0.AddMinutes(5), sameCron.NextRunAt);
        Assert.Equal(T0, sameCron.CreatedAt);
        Assert.Equal(T0.AddMinutes(1), sameCron.UpdatedAt);
        Assert.Equal(2, sameCron.Args.GetProperty("v").GetInt32());

        var newCron = await Upsert("r1", "0 * * * *", T0.AddHours(1), now: T0.AddMinutes(2));
        Assert.Equal(T0.AddHours(1), newCron.NextRunAt);
        var read = (await _store.GetRecurringAsync("r1", Ct))!;
        Assert.Equal((newCron.Cron, newCron.NextRunAt, newCron.CreatedAt, newCron.UpdatedAt), (read.Cron, read.NextRunAt, read.CreatedAt, read.UpdatedAt));
    }

    [Fact]
    public async Task Recurring_list_due_next_and_remove()
    {
        await Upsert("b", "* * * * *", T0.AddMinutes(2));
        await Upsert("a", "* * * * *", T0.AddMinutes(3));
        await Upsert("c", "* * * * *", T0.AddMinutes(1));

        Assert.Equal(["a", "b", "c"], (await _store.ListRecurringAsync(Ct)).Select(r => r.Id));
        Assert.Equal(["c", "b"], (await _store.DueRecurringAsync(T0.AddMinutes(2), 10, [], Ct)).Select(r => r.Id));
        Assert.Equal(["b"], (await _store.DueRecurringAsync(T0.AddMinutes(2), 10, ["c"], Ct)).Select(r => r.Id));
        Assert.Equal(["c"], (await _store.DueRecurringAsync(T0.AddMinutes(5), 1, [], Ct)).Select(r => r.Id));
        Assert.Equal(T0.AddMinutes(2), await _store.NextRecurringAtAsync(["c"], Ct));

        Assert.True(await _store.RemoveRecurringAsync("c", Ct));
        Assert.False(await _store.RemoveRecurringAsync("c", Ct));
        Assert.Null(await _store.GetRecurringAsync("c", Ct));
        Assert.Equal(T0.AddMinutes(2), await _store.NextRecurringAtAsync([], Ct));
    }

    [Fact]
    public async Task Fire_creates_the_job_and_advances_next_run()
    {
        var definition = await Upsert("r", "* * * * *", T0.AddMinutes(1));
        var job = RecurringJob(definition, T0.AddMinutes(1));

        Assert.Equal(RecurringFireResult.Fired,
            await _store.FireRecurringAsync(new RecurringFire(definition, definition.NextRunAt, T0.AddMinutes(2), job), Ct));
        var stored = (await _store.GetRecurringAsync("r", Ct))!;
        Assert.Equal(T0.AddMinutes(2), stored.NextRunAt);
        Assert.Equal(job.Now, stored.LastRunAt);
        Assert.Equal(job.Id, stored.LastJobId);
        Assert.Equal(JobState.Enqueued, (await Get(job.Id)).State);
    }

    [Fact]
    public async Task Fire_is_stale_after_another_fire_or_an_upsert_and_gone_after_remove()
    {
        var definition = await Upsert("r", "* * * * *", T0.AddMinutes(1));
        var fire = new RecurringFire(definition, definition.NextRunAt, T0.AddMinutes(2), RecurringJob(definition, T0.AddMinutes(1)));
        Assert.Equal(RecurringFireResult.Fired, await _store.FireRecurringAsync(fire, Ct));

        var second = fire with { Job = RecurringJob(definition, T0.AddMinutes(1)) };
        Assert.Equal(RecurringFireResult.Stale, await _store.FireRecurringAsync(second, Ct));
        Assert.Null(await _store.GetAsync(second.Job.Id, Ct));

        var current = (await _store.GetRecurringAsync("r", Ct))!;
        await Upsert("r", "* * * * *", T0.AddMinutes(9), now: T0.AddSeconds(30));
        var afterUpsert = new RecurringFire(current, null, null, RecurringJob(current, T0.AddMinutes(1)));
        Assert.Equal(RecurringFireResult.Stale, await _store.FireRecurringAsync(afterUpsert, Ct));

        await _store.RemoveRecurringAsync("r", Ct);
        Assert.Equal(RecurringFireResult.Gone, await _store.FireRecurringAsync(afterUpsert, Ct));
    }

    [Fact]
    public async Task Manual_fire_leaves_next_run_alone()
    {
        var definition = await Upsert("r", "* * * * *", T0.AddMinutes(1));
        var job = RecurringJob(definition, T0.AddSeconds(10));
        Assert.Equal(RecurringFireResult.Fired, await _store.FireRecurringAsync(new RecurringFire(definition, null, null, job), Ct));
        var stored = (await _store.GetRecurringAsync("r", Ct))!;
        Assert.Equal(T0.AddMinutes(1), stored.NextRunAt);
        Assert.Equal(job.Id, stored.LastJobId);
    }

    [Fact]
    public async Task Concurrent_fires_of_one_occurrence_create_one_job()
    {
        var definition = await Upsert("r", "* * * * *", T0.AddMinutes(1));
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => _store.FireRecurringAsync(
            new RecurringFire(definition, definition.NextRunAt, T0.AddMinutes(2), RecurringJob(definition, T0.AddMinutes(1))), Ct)));
        Assert.Single(results, r => r == RecurringFireResult.Fired);
        Assert.Single(await _store.ListAsync(null, 100, false, Ct));
    }

    // ---- helpers ----

    private static DateTime Truncate(DateTime t) => new(t.Ticks - t.Ticks % TimeSpan.TicksPerMillisecond, DateTimeKind.Utc);

    private Task<Guid> Insert(
        DateTime? runAt = null, string queue = JobQueues.Default, int priority = 0, string? key = null, string name = "job",
        string args = "{}", int maxAttempts = 3) =>
        _store.InsertAsync(New(runAt, queue, priority, key, name, args, maxAttempts), Ct);

    private static NewJob New(
        DateTime? runAt = null, string queue = JobQueues.Default, int priority = 0, string? key = null, string name = "job",
        string args = "{}", int maxAttempts = 3) =>
        new(Guid.CreateVersion7(), name, args, 1, maxAttempts, 1000, 0, runAt, T0, queue, priority, key);

    private async Task<Job> Get(Guid id) => await _store.GetAsync(id, Ct) ?? throw new InvalidOperationException($"job {id} missing");

    private static List<(JobState, string)> History(Job job) => job.History.Select(h => (h.State, h.Reason)).ToList();

    private Task<IReadOnlyList<ClaimedJob>> Claim(
        int limit = 1, ClaimOrder order = ClaimOrder.Priority, string node = Node, string queue = JobQueues.Default,
        IReadOnlyCollection<string>? excluded = null, TimeSpan? lease = null, DateTime? now = null,
        IReadOnlyDictionary<string, int>? limits = null, IReadOnlyCollection<string>? names = null) =>
        _store.ClaimAsync(
            new ClaimRequest(queue, order, node, now ?? T0, lease ?? Lease, limit, excluded ?? [], limits ?? new Dictionary<string, int>())
            {
                Names = names,
            }, Ct);

    private Task<bool> Finish(
        ClaimedJob job, JobTransition transition, AttemptOutcome outcome, string node = Node, string? error = null, DateTime? retryAt = null) =>
        _store.FinishAsync(job, node, transition, outcome, error, T0.AddSeconds(1), retryAt, Ct);

    /// Leases are measured on the store's clock (the database's, for Postgres), so expiry uses real time: a short
    /// lease taken now, then a wait past it.
    private async Task<ClaimedJob> ClaimAndExpire(Func<Task>? beforeExpiry = null)
    {
        var lease = TimeSpan.FromMilliseconds(200);
        var claimed = Assert.Single(await Claim(lease: lease, now: DateTime.UtcNow));
        if (beforeExpiry != null) await beforeExpiry();
        await Task.Delay(lease * 3);
        return claimed;
    }

    private Task<RecurringJob> Upsert(string id, string cron, DateTime nextRunAt, DateTime? now = null, string args = "{}") =>
        _store.UpsertRecurringAsync(
            new RecurringDefinition(id, "job", cron, "UTC", args, 1, 3, 1000, 0, JobQueues.Default, 0, nextRunAt, now ?? T0), Ct);

    private static NewJob RecurringJob(RecurringJob definition, DateTime now) => new(
        Guid.CreateVersion7(), definition.JobName, definition.Args.GetRawText(), definition.ArgsVersion, definition.MaxAttempts,
        (int)definition.RetryDelay.TotalMilliseconds, (int)definition.RetryJitter.TotalMilliseconds, null, now, definition.Queue,
        definition.Priority);
}
