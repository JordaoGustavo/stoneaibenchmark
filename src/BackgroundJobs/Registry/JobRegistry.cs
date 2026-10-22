using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace BackgroundJobs;

public sealed class JobRegistrationOptions
{
    /// Opt-in per-type breaker: after N consecutive failures stop claiming this type for the break duration.
    public CircuitBreakerOptions? CircuitBreaker { get; set; }
}

public sealed record CircuitBreakerOptions(int FailureThreshold, TimeSpan BreakDuration);

/// Maps stable job names to implementations. Registering after the server started is allowed.
public sealed class JobRegistry
{
    private readonly ConcurrentDictionary<string, JobRegistration> _jobs = new();

    public JobRegistry Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null)
        where TJob : IJob<TArgs>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var options = new JobRegistrationOptions();
        configure?.Invoke(options);
        var registration = new JobRegistration(name, typeof(TArgs), options, async (services, args, context) =>
        {
            var typedArgs = args.Deserialize<TArgs>(JobJson.Options)!;
            var job = ActivatorUtilities.GetServiceOrCreateInstance<TJob>(services);
            await job.ExecuteAsync(typedArgs, context);
        });
        if (!_jobs.TryAdd(name, registration)) throw new ArgumentException($"job '{name}' is already registered");
        return this;
    }

    internal JobRegistration? Find(string name) => _jobs.GetValueOrDefault(name);

    internal IEnumerable<JobRegistration> All => _jobs.Values;

    /// Registered job names with their argument types, ordered by name.
    public IReadOnlyList<(string Name, Type ArgsType)> Registrations =>
        _jobs.Values.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => (r.Name, r.ArgsType)).ToList();
}

internal sealed record JobRegistration(
    string Name,
    Type ArgsType,
    JobRegistrationOptions Options,
    Func<IServiceProvider, JsonElement, JobContext, Task> Invoke);

internal static class JobJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
