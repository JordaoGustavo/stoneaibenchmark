using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;

namespace BackgroundJobs;

public sealed class JobRegistrationOptions
{
    private readonly Dictionary<int, Func<JsonObject, JsonObject>> _upgrades = [];

    /// Opt-in per-type breaker: after N consecutive failures stop claiming this type for the break duration.
    public CircuitBreakerOptions? CircuitBreaker { get; set; }

    /// Version of the args shape this code reads and writes. Every job is stored with the version it was enqueued with.
    /// Bump it on a breaking change to the args type and register an <see cref="UpgradeArgs"/> step from the previous version.
    public int ArgsVersion { get; set; } = 1;

    /// Rewrites args stored as <paramref name="fromVersion"/> into the shape of <paramref name="fromVersion"/> + 1.
    /// Applied in order when a job with older args runs; the stored args are left as they were.
    public JobRegistrationOptions UpgradeArgs(int fromVersion, Func<JsonObject, JsonObject> upgrade)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(fromVersion, 1);
        ArgumentNullException.ThrowIfNull(upgrade);
        if (!_upgrades.TryAdd(fromVersion, upgrade)) throw new ArgumentException($"an args upgrade from version {fromVersion} is already registered");
        return this;
    }

    internal IReadOnlyDictionary<int, Func<JsonObject, JsonObject>> Upgrades => _upgrades;
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
        if (options.ArgsVersion < 1) throw new ArgumentException($"job '{name}': ArgsVersion must be >= 1");
        var missing = Enumerable.Range(1, options.ArgsVersion - 1).Where(v => !options.Upgrades.ContainsKey(v)).ToList();
        if (missing.Count > 0)
            throw new ArgumentException($"job '{name}': ArgsVersion is {options.ArgsVersion} but there is no args upgrade from version {string.Join(", ", missing)}");
        if (options.Upgrades.Keys.Any(v => v >= options.ArgsVersion))
            throw new ArgumentException($"job '{name}': args upgrades must start below ArgsVersion {options.ArgsVersion}");

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
    Func<IServiceProvider, JsonElement, JobContext, Task> InvokeCurrent)
{
    /// Brings args stored as <paramref name="version"/> to the registered version, then runs the job.
    public Task Invoke(IServiceProvider services, JsonElement args, int version, JobContext context)
    {
        if (version > Options.ArgsVersion)
            throw new InvalidOperationException($"args version {version} of job '{Name}' is newer than the registered version {Options.ArgsVersion}");
        if (version == Options.ArgsVersion) return InvokeCurrent(services, args, context);

        var node = JsonNode.Parse(args.GetRawText()) as JsonObject
                   ?? throw new InvalidOperationException($"args of job '{Name}' are not a JSON object and cannot be upgraded");
        for (var v = version; v < Options.ArgsVersion; v++) node = Options.Upgrades[v](node);
        return InvokeCurrent(services, JsonSerializer.SerializeToElement(node, JobJson.Options), context);
    }

    /// Enqueuing under a registered name must pass that registration's args type (or raw JSON in the current shape).
    public void CheckArgs(object? args)
    {
        if (args is null or JsonElement or JsonNode || ArgsType.IsInstanceOfType(args)) return;
        throw new ArgumentException($"job '{Name}' takes {ArgsType.Name} args, not {args.GetType().Name}");
    }
}

/// Serializer options used for job args, so callers that build args by hand serialize them the same way.
public static class JobJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
