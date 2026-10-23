using BackgroundJobs.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace BackgroundJobs;

/// <summary>
/// The PostgreSQL store (BackgroundJobs.Postgres).
/// </summary>
public static class PostgresBuilderExtensions
{
    /// <summary>
    /// Stores jobs in PostgreSQL. The schema is created and migrated at startup; nodes sharing the database share the queue.
    /// </summary>
    public static BackgroundJobsBuilder UsePostgres(this BackgroundJobsBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        try { _ = new NpgsqlConnectionStringBuilder(connectionString); }
        catch (ArgumentException e)
        {
            throw new ArgumentException(
                $"UsePostgres: the connection string is malformed ({e.Message}); pass an Npgsql connection string such as " +
                "\"Host=localhost;Database=app;Username=app;Password=...\"", nameof(connectionString), e);
        }
        var services = builder.Services;
        services.AddSingleton(_ => new Postgres.JobDataSource(NpgsqlDataSource.Create(connectionString)));
        services.Replace(ServiceDescriptor.Singleton<IJobStore>(sp => new Postgres.PostgresJobStore(sp.GetRequiredService<Postgres.JobDataSource>().Db)));
        services.Replace(ServiceDescriptor.Singleton<IJobStoreListener>(sp => ActivatorUtilities.CreateInstance<Postgres.PostgresJobListener>(sp)));
        return builder;
    }
}
