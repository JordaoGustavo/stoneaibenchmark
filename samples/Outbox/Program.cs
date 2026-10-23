// Transactional enqueue (outbox) on Postgres: the order row and its jobs commit or roll back together.
// Also shows batch enqueue (EnqueueManyAsync), inside the transaction and on its own.
using BackgroundJobs;
using Npgsql;

const string Queue = "sample-outbox";
var connectionString = Environment.GetEnvironmentVariable("DATABASE_URL")
                       ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs";

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; },
        registerJobs: jobs => jobs
            .Register<SendReceiptJob, OrderArgs>("send-receipt")
            .Register<ReserveStockJob, OrderArgs>("reserve-stock"))
    .UsePostgres(connectionString);

var host = builder.Build();
await host.StartAsync();
var client = host.Services.GetRequiredService<IJobClient>();

// The application's own table, in the same database the jobs use.
await using var conn = new NpgsqlConnection(connectionString);
await conn.OpenAsync();
await using (var create = new NpgsqlCommand(
                 "CREATE TABLE IF NOT EXISTS sample_orders (id uuid PRIMARY KEY, total_cents int NOT NULL)", conn))
    await create.ExecuteNonQueryAsync();

// 1) Commit: the order and both jobs (one batch, via EnqueueManyAsync) become visible at once; workers wake on commit.
var orderId = Guid.NewGuid();
IReadOnlyList<Guid> committed;
await using (var tx = await conn.BeginTransactionAsync())
{
    await InsertOrder(conn, tx, orderId, 4200);
    committed = await client.EnqueueManyAsync(tx,
    [
        new JobToEnqueue("reserve-stock", new OrderArgs(orderId), new JobOptions { Queue = Queue }),
        new JobToEnqueue("send-receipt", new OrderArgs(orderId), new JobOptions { Queue = Queue }),
    ]);
    await tx.CommitAsync();
}

// 2) Rollback: neither the order nor the job exists afterwards.
var rolledBackOrder = Guid.NewGuid();
Guid rolledBack;
await using (var tx = await conn.BeginTransactionAsync())
{
    await InsertOrder(conn, tx, rolledBackOrder, 999);
    rolledBack = await client.EnqueueAsync(tx, "send-receipt", new OrderArgs(rolledBackOrder), new JobOptions { Queue = Queue });
    await tx.RollbackAsync(); // e.g. a validation error after the insert
}
try { await client.GetAsync(rolledBack); Console.WriteLine("unexpected: rolled-back job exists"); }
catch (JobNotFoundException) { Console.WriteLine($"rolled-back job {rolledBack}: not found (as expected)"); }

// 3) Batch outside a transaction: atomic (all or none), ids returned in input order.
var batch = await client.EnqueueManyAsync(Enumerable.Range(0, 3)
    .Select(_ => new JobToEnqueue("send-receipt", new OrderArgs(Guid.NewGuid()), new JobOptions { Queue = Queue })));

foreach (var id in committed.Concat(batch))
{
    Job job;
    while ((job = await client.GetAsync(id)).State is not (JobState.Succeeded or JobState.Failed)) await Task.Delay(100);
    Console.WriteLine($"{job.Name,-13} {id} {job.State}");
}

await host.StopAsync();

static async Task InsertOrder(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, int totalCents)
{
    await using var cmd = new NpgsqlCommand("INSERT INTO sample_orders (id, total_cents) VALUES ($1, $2)", conn, tx);
    cmd.Parameters.Add(new() { Value = id });
    cmd.Parameters.Add(new() { Value = totalCents });
    await cmd.ExecuteNonQueryAsync();
}

public sealed record OrderArgs(Guid OrderId);

public sealed class SendReceiptJob(ILogger<SendReceiptJob> logger) : IJob<OrderArgs>
{
    public Task ExecuteAsync(OrderArgs args, JobContext context)
    {
        logger.LogInformation("receipt sent for order {OrderId}", args.OrderId);
        return Task.CompletedTask;
    }
}

public sealed class ReserveStockJob(ILogger<ReserveStockJob> logger) : IJob<OrderArgs>
{
    public Task ExecuteAsync(OrderArgs args, JobContext context)
    {
        logger.LogInformation("stock reserved for order {OrderId}", args.OrderId);
        return Task.CompletedTask;
    }
}
