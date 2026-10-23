using BackgroundJobs.Processing;
using Npgsql;

namespace BackgroundJobs;

/// <summary>
/// Transactional enqueue (outbox pattern) for the PostgreSQL store: the job is written on the application's own
/// <see cref="NpgsqlTransaction"/>, so it exists only if that transaction commits, together with the application's
/// writes. The transaction must be open on the database given to <c>UsePostgres</c>.
/// </summary>
/// <remarks>
/// Workers are woken by a NOTIFY that PostgreSQL delivers on commit (and drops on rollback), on every node; nothing
/// runs before the commit. Idempotency keys behave as in <see cref="IJobClient.EnqueueAsync{TArgs}"/>, including a key
/// held by a job enqueued earlier on the same transaction; an insert whose key is held by another transaction not yet
/// committed waits for it. Use READ COMMITTED (PostgreSQL's default): under REPEATABLE READ or SERIALIZABLE such a
/// conflict fails with a serialization error the application must retry. Rolling back frees the key.
/// </remarks>
public static class PostgresJobClientExtensions
{
    /// <summary>
    /// <see cref="IJobClient.EnqueueAsync{TArgs}"/> on <paramref name="transaction"/>: the job is created when it commits.
    /// Returns the job's id, or the id of the unfinished job holding <see cref="JobOptions.IdempotencyKey"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The client does not use the PostgreSQL store, or the transaction is completed.</exception>
    public static async Task<Guid> EnqueueAsync<TArgs>(
        this IJobClient client, NpgsqlTransaction transaction, string jobName, TArgs args, JobOptions? options = null, CancellationToken ct = default) =>
        (await client.EnqueueManyAsync(transaction, [new JobToEnqueue(jobName, args, options)], ct))[0];

    /// <summary>
    /// <see cref="IJobClient.EnqueueManyAsync(IEnumerable{JobToEnqueue}, CancellationToken)"/> on <paramref name="transaction"/>:
    /// the jobs are created when it commits. Ids in input order.
    /// </summary>
    /// <exception cref="InvalidOperationException">The client does not use the PostgreSQL store, or the transaction is completed.</exception>
    public static async Task<IReadOnlyList<Guid>> EnqueueManyAsync(
        this IJobClient client, NpgsqlTransaction transaction, IEnumerable<JobToEnqueue> jobs, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(transaction);
        if (client is not JobClient { Store: Postgres.PostgresJobStore } jobClient)
            throw new InvalidOperationException(
                "enqueueing on an NpgsqlTransaction needs the PostgreSQL store: chain .UsePostgres(connectionString) after AddBackgroundJobs " +
                "(with .UseInMemoryStorage() call EnqueueAsync without a transaction)");
        var connection = transaction.Connection
                         ?? throw new InvalidOperationException("the NpgsqlTransaction is already committed or rolled back; enqueue before committing it");
        var batch = jobClient.PrepareMany(jobs);
        if (batch.Count == 0) return [];
        // No local wake-up here: the jobs do not exist until the commit, whose NOTIFY wakes this node's listener too.
        return await Postgres.PostgresBatchInsert.InsertAsync(connection, transaction, batch, ct);
    }
}
