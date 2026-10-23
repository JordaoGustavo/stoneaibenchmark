using BackgroundJobs.Health;
using BackgroundJobs.Processing;
using BackgroundJobs.Storage;
using BackgroundJobs.Storage.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace BackgroundJobs;

/// Returned by AddBackgroundJobs; pick a store on it: UsePostgres(...) (BackgroundJobs.Postgres) or UseInMemoryStorage().
public sealed class BackgroundJobsBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = services;

    /// Keeps jobs in this process instead of a database: for tests and local runs of code that integrates the library.
    /// Everything is lost when the process exits and nodes share nothing, so never use it in production.
    public BackgroundJobsBuilder UseInMemoryStorage()
    {
        Services.Replace(ServiceDescriptor.Singleton<IJobStore>(sp => new InMemoryJobStore()));
        Services.Replace(ServiceDescriptor.Singleton<IJobStoreListener, NoListener>());
        return this;
    }

    /// Declares a recurring job, created or updated (<see cref="IRecurringJobClient.UpsertAsync"/>) every time the server
    /// starts. Declarations removed from code are not deleted from the store: use <see cref="IRecurringJobClient.RemoveAsync"/>.
    /// <exception cref="ArgumentException">Invalid id or cron.</exception>
    public BackgroundJobsBuilder AddRecurringJob<TArgs>(string id, string jobName, string cron, TArgs args, RecurringJobOptions? options = null)
    {
        RecurringJobIds.Check(id);
        if (!CronExpression.TryParse(cron, out _)) throw new ArgumentException($"recurring job '{id}': cron '{cron}' is invalid");
        Services.AddSingleton(new RecurringSeed(id, (client, ct) => client.UpsertAsync(id, jobName, cron, args, options, ct)));
        return this;
    }

    /// No other node can change the in-memory store, so there is nothing to listen for.
    private sealed class NoListener : IJobStoreListener
    {
        public Task RunAsync(CancellationToken ct) => Task.CompletedTask;
    }
}

public static class ServiceCollectionExtensions
{
    /// Adds the job client (<see cref="IJobClient"/>), the <see cref="JobRegistry"/> and the hosted job server.
    /// Chain a store: <c>.UsePostgres(connectionString)</c> or <c>.UseInMemoryStorage()</c>.
    public static BackgroundJobsBuilder AddBackgroundJobs(
        this IServiceCollection services, Action<BackgroundJobsOptions>? configure = null, Action<JobRegistry>? registerJobs = null)
    {
        services.AddOptions<BackgroundJobsOptions>().Configure(configure ?? (_ => { })).Validate(o => { o.Validate(); return true; }).ValidateOnStart();

        var registry = new JobRegistry();
        registerJobs?.Invoke(registry);
        services.AddSingleton(registry);

        services.TryAddSingleton(TimeProvider.System);
        // Replaced by the store the caller picks.
        services.AddSingleton<IJobStore>(_ => throw NoStore());
        services.AddSingleton<IJobStoreListener>(_ => throw NoStore());
        services.AddMetrics();
        services.AddSingleton<Signals>();
        services.AddSingleton<IJobSignals>(sp => sp.GetRequiredService<Signals>());
        services.AddSingleton<StoreCircuitBreaker>();
        services.AddSingleton<JobCircuitBreaker>();
        services.AddSingleton<CancelRequests>();
        services.AddSingleton<AbandonedJobs>();
        services.AddSingleton<JobExecutor>();
        services.AddSingleton<Dispatcher>();
        services.AddSingleton<Scheduler>();
        services.AddSingleton<LeaseHeartbeat>();
        services.AddSingleton<QueueMonitor>();
        services.AddSingleton<RetentionSweeper>();
        services.AddSingleton<JobsMetrics>();
        services.AddSingleton<IJobClient, JobClient>();
        services.AddSingleton<IRecurringJobClient, RecurringJobClient>();
        services.AddSingleton<JobServer>();
        services.AddHostedService(sp => sp.GetRequiredService<JobServer>());
        services.AddSingleton(sp => new JobServerHealthCheck(
            sp.GetRequiredService<JobServer>(), sp.GetRequiredService<JobCircuitBreaker>(), sp.GetRequiredService<QueueMonitor>(),
            sp.GetRequiredService<AbandonedJobs>(), sp.GetRequiredService<IOptions<BackgroundJobsOptions>>(), sp.GetRequiredService<TimeProvider>()));
        return new BackgroundJobsBuilder(services);
    }

    private static InvalidOperationException NoStore() =>
        new("no job store configured: chain .UsePostgres(connectionString) (BackgroundJobs.Postgres) or .UseInMemoryStorage() after AddBackgroundJobs");
}
