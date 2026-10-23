namespace BackgroundJobs;

/// <summary>
/// Enqueues and manages jobs. Registered as a singleton by AddBackgroundJobs.
/// </summary>
public interface IJobClient
{
    /// <summary>
    /// Persists the job and returns its id. Unknown names are accepted and fail when executed. With
    /// <see cref="JobOptions.IdempotencyKey"/> held by an unfinished job, persists nothing and returns that job's id.
    /// </summary>
    Task<Guid> EnqueueAsync<TArgs>(string jobName, TArgs args, JobOptions? options = null, CancellationToken ct = default);

    /// <summary>
    /// The job with its args, history and attempts.
    /// </summary>
    /// <exception cref="JobNotFoundException"/>
    Task<Job> GetAsync(Guid id, CancellationToken ct = default);

    /// <summary>
    /// By id (creation order), ascending unless <paramref name="descending"/>. Pass the last id seen as <paramref name="after"/> for the next page.
    /// <paramref name="limit"/> is 1 to 1000.
    /// </summary>
    Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, bool descending = false, CancellationToken ct = default);

    /// <summary>
    /// Deletes a scheduled or enqueued job before it runs. The job stays readable, as deleted, until retention purges it.
    /// </summary>
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task DeleteAsync(Guid id, CancellationToken ct = default);

    /// <summary>
    /// Runs a finished job again from attempt 1, keeping its history.
    /// </summary>
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    /// <exception cref="IdempotencyKeyConflictException"/>
    Task RequeueAsync(Guid id, CancellationToken ct = default);

    /// <summary>
    /// Stops a job for good: a scheduled or enqueued one at once; a processing one by firing its
    /// <see cref="JobContext.CancellationToken"/> — at once on this node, within LeaseDuration / 3 on another — after which
    /// it ends cancelled. A job that ignores the token and completes is recorded as it ended.
    /// </summary>
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task CancelAsync(Guid id, CancellationToken ct = default);
}
