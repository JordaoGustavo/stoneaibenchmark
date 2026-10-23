using BackgroundJobs.Storage;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;

namespace BackgroundJobs.StoreContract;

/// One Postgres container for the whole run; every test gets its own empty, migrated database.
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public async Task<string> CreateDatabaseAsync()
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        await using (var conn = new NpgsqlConnection(_container.GetConnectionString()))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {name}", conn);
            await cmd.ExecuteNonQueryAsync();
        }
        return new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Database = name }.ConnectionString;
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}

/// The stores are internal: they are built the way an application gets them, through AddBackgroundJobs.
internal static class StoreFactory
{
    public static async Task<(IJobStore Store, IAsyncDisposable Owner)> CreateAsync(Action<BackgroundJobsBuilder> useStore)
    {
        var services = new ServiceCollection();
        useStore(services.AddBackgroundJobs());
        var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IJobStore>();
        await store.MigrateAsync(CancellationToken.None);
        return (store, provider);
    }
}

public sealed class InMemoryJobStoreContract : JobStoreContract
{
    protected override Task<(IJobStore Store, IAsyncDisposable Owner)> CreateStoreAsync() =>
        StoreFactory.CreateAsync(b => b.UseInMemoryStorage());
}

[Collection(PostgresCollection.Name)]
public sealed class PostgresJobStoreContract(PostgresFixture postgres) : JobStoreContract
{
    protected override async Task<(IJobStore Store, IAsyncDisposable Owner)> CreateStoreAsync()
    {
        var connectionString = await postgres.CreateDatabaseAsync();
        return await StoreFactory.CreateAsync(b => b.UsePostgres(connectionString));
    }
}
