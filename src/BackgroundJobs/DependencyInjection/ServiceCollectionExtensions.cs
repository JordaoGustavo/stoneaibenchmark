using BackgroundJobs.Health;
using BackgroundJobs.Processing;
using BackgroundJobs.Storage;
using BackgroundJobs.Storage.Postgres;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;

namespace BackgroundJobs;

public static class ServiceCollectionExtensions
{
    /// Adds the job client (<see cref="IJobClient"/>), the <see cref="JobRegistry"/> and the hosted job server.
    public static IServiceCollection AddBackgroundJobs(
        this IServiceCollection services, Action<BackgroundJobsOptions> configure, Action<JobRegistry>? registerJobs = null)
    {
        services.AddOptions<BackgroundJobsOptions>().Configure(configure).Validate(o => { o.Validate(); return true; });

        var registry = new JobRegistry();
        registerJobs?.Invoke(registry);
        services.AddSingleton(registry);

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(sp => new JobDataSource(
            NpgsqlDataSource.Create(sp.GetRequiredService<IOptions<BackgroundJobsOptions>>().Value.ConnectionString)));
        services.AddSingleton<IJobStore>(sp => new PostgresJobStore(sp.GetRequiredService<JobDataSource>().Db));
        services.AddSingleton<PostgresJobListener>();
        services.AddMetrics();
        services.AddSingleton<Signals>();
        services.AddSingleton<StoreCircuitBreaker>();
        services.AddSingleton<JobCircuitBreaker>();
        services.AddSingleton<JobExecutor>();
        services.AddSingleton<Dispatcher>();
        services.AddSingleton<Scheduler>();
        services.AddSingleton<LeaseHeartbeat>();
        services.AddSingleton<QueueMonitor>();
        services.AddSingleton<JobsMetrics>();
        services.AddSingleton<IJobClient, JobClient>();
        services.AddSingleton<JobServer>();
        services.AddHostedService(sp => sp.GetRequiredService<JobServer>());
        services.AddSingleton(sp => new JobServerHealthCheck(
            sp.GetRequiredService<JobServer>(), sp.GetRequiredService<JobCircuitBreaker>(), sp.GetRequiredService<QueueMonitor>(),
            sp.GetRequiredService<IOptions<BackgroundJobsOptions>>(), sp.GetRequiredService<TimeProvider>()));
        return services;
    }
}
