using Npgsql;
using Testcontainers.PostgreSql;

namespace BackgroundJobs.E2E.Infra;

/// One Postgres container for the whole run; every test gets its own empty database.
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// What DemoProcess.StartAsync takes for <paramref name="store"/>: a fresh database, or null for the in-memory store.
    public async Task<string?> CreateStoreAsync(string store) => store switch
    {
        Stores.Postgres => await CreateDatabaseAsync(),
        Stores.Memory => null,
        _ => throw new ArgumentOutOfRangeException(nameof(store)),
    };

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

/// Scenarios that do not depend on durability run against every store: the same observable behaviour is the
/// contract a store must meet. Restart, crash and stop scenarios need a durable store and stay Postgres-only.
public static class Stores
{
    public const string Postgres = "postgres";
    public const string Memory = "memory";

    public static TheoryData<string> All => new() { Postgres, Memory };
}

[CollectionDefinition(Name)]
public sealed class E2ECollection : ICollectionFixture<PostgresFixture>
{
    // Scenarios run sequentially: they measure timings and spawn real processes.
    public const string Name = "e2e";
}
