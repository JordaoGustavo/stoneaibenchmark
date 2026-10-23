using BackgroundJobs.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace BackgroundJobs;

public static class PostgresBuilderExtensions
{
    /// Stores jobs in PostgreSQL. The schema is created and migrated at startup; nodes sharing the database share the queue.
    public static BackgroundJobsBuilder UsePostgres(this BackgroundJobsBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        var services = builder.Services;
        services.AddSingleton(_ => new Postgres.JobDataSource(NpgsqlDataSource.Create(connectionString)));
        services.Replace(ServiceDescriptor.Singleton<IJobStore>(sp => new Postgres.PostgresJobStore(sp.GetRequiredService<Postgres.JobDataSource>().Db)));
        services.Replace(ServiceDescriptor.Singleton<IJobStoreListener>(sp => ActivatorUtilities.CreateInstance<Postgres.PostgresJobListener>(sp)));
        return builder;
    }
}
