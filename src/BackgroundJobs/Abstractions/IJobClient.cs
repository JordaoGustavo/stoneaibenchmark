namespace BackgroundJobs;

public interface IJobClient
{
    /// Persists the job and returns its id. Unknown names are accepted and fail when executed.
    Task<Guid> EnqueueAsync<TArgs>(string jobName, TArgs args, JobOptions? options = null, CancellationToken ct = default);

    /// <exception cref="JobNotFoundException"/>
    Task<Job> GetAsync(Guid id, CancellationToken ct = default);

    /// Ascending by id (creation order). Pass the last id seen as <paramref name="after"/> for the next page.
    Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, CancellationToken ct = default);

    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task DeleteAsync(Guid id, CancellationToken ct = default);

    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task RequeueAsync(Guid id, CancellationToken ct = default);

    /// Stops a job for good: a scheduled or enqueued one at once; a processing one by firing its
    /// <see cref="JobContext.CancellationToken"/> — at once on this node, within LeaseDuration / 3 on another — after which
    /// it ends cancelled. A job that ignores the token and completes is recorded as it ended.
    /// <exception cref="JobNotFoundException"/> <exception cref="InvalidJobTransitionException"/>
    Task CancelAsync(Guid id, CancellationToken ct = default);
}
