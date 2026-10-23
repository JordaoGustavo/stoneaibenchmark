using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;

namespace BackgroundJobs;

/// <summary>
/// Per-name settings, configured in <see cref="JobRegistry.Register{TJob, TArgs}"/>.
/// </summary>
public sealed class JobRegistrationOptions
{
    private readonly Dictionary<int, Func<JsonObject, JsonObject>> _upgrades = [];

    /// <summary>
    /// Opt-in per-type breaker: after N consecutive failures stop claiming this type for the break duration.
    /// </summary>
    public CircuitBreakerOptions? CircuitBreaker { get; set; }

    /// <summary>
    /// Version of the args shape this code reads and writes. Every job is stored with the version it was enqueued with.
    /// Bump it on a breaking change to the args type and register an <see cref="UpgradeArgs"/> step from the previous version.
    /// </summary>
    public int ArgsVersion { get; set; } = 1;

    /// <summary>
    /// Most jobs of this name processing at once, across every node and queue; null: no limit. Jobs over it stay enqueued
    /// while others are claimed past them. Set it alike on every node: a node without it claims this name freely.
    /// </summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    /// Overrides <see cref="BackgroundJobsOptions.DefaultJobTimeout"/> for this job name.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// Rewrites args stored as <paramref name="fromVersion"/> into the shape of <paramref name="fromVersion"/> + 1.
    /// Applied in order when a job with older args runs; the stored args are left as they were.
    /// </summary>
    public JobRegistrationOptions UpgradeArgs(int fromVersion, Func<JsonObject, JsonObject> upgrade)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(fromVersion, 1);
        ArgumentNullException.ThrowIfNull(upgrade);
        if (!_upgrades.TryAdd(fromVersion, upgrade)) throw new ArgumentException(
            $"an args upgrade from version {fromVersion} is already registered; call UpgradeArgs once per version, each one rewriting version n into n + 1");
        return this;
    }

    internal IReadOnlyDictionary<int, Func<JsonObject, JsonObject>> Upgrades => _upgrades;
}

/// <summary>
/// See <see cref="JobRegistrationOptions.CircuitBreaker"/>.
/// </summary>
/// <param name="FailureThreshold">Consecutive failed attempts of the name that open the breaker.</param>
/// <param name="BreakDuration">How long the name is not claimed once open; then one trial attempt is let through.</param>
public sealed record CircuitBreakerOptions(int FailureThreshold, TimeSpan BreakDuration);

/// <summary>
/// Maps stable job names to implementations. Registering after the server started is allowed.
/// </summary>
public sealed class JobRegistry
{
    private readonly ConcurrentDictionary<string, JobRegistration> _jobs = new();
    private volatile IReadOnlyDictionary<string, int> _concurrencyLimits = new Dictionary<string, int>();
    private volatile string[] _names = [];

    /// <summary>
    /// Maps <paramref name="name"/> to <typeparamref name="TJob"/>, resolved from DI per attempt (or created with
    /// ActivatorUtilities when not registered). The name is stored with every job: keep it stable across deploys.
    /// </summary>
    /// <exception cref="ArgumentException">The name is taken, or <paramref name="configure"/> set invalid options.</exception>
    public JobRegistry Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null)
        where TJob : IJob<TArgs>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var options = new JobRegistrationOptions();
        configure?.Invoke(options);
        if (options.ArgsVersion < 1)
            throw new ArgumentException($"job '{name}': ArgsVersion is {options.ArgsVersion}, but it must be at least 1 (the default); fix it in Register(\"{name}\", o => o.ArgsVersion = ...)");
        var missing = Enumerable.Range(1, options.ArgsVersion - 1).Where(v => !options.Upgrades.ContainsKey(v)).ToList();
        if (missing.Count > 0)
            throw new ArgumentException(
                $"job '{name}': ArgsVersion is {options.ArgsVersion} but there is no args upgrade from version {string.Join(", ", missing)}, so older " +
                $"jobs could not run; add o.UpgradeArgs({missing[0]}, args => ...) for each missing version in Register(\"{name}\", o => ...)");
        if (options.Upgrades.Keys.Where(v => v >= options.ArgsVersion).ToList() is [_, ..] extra)
            throw new ArgumentException(
                $"job '{name}': there is an args upgrade from version {string.Join(", ", extra)}, not below ArgsVersion {options.ArgsVersion}; " +
                $"set o.ArgsVersion = {extra.Max() + 1} if it is the current shape, or remove the upgrade");
        if (options.MaxConcurrency < 1)
            throw new ArgumentException($"job '{name}': MaxConcurrency is {options.MaxConcurrency}, but it must be at least 1; leave it null for no limit");
        if (options.Timeout is { } timeout && !BackgroundJobsOptions.IsValidTimeout(timeout))
            throw new ArgumentException(
                $"job '{name}': Timeout is {timeout}, but it must be positive or Timeout.InfiniteTimeSpan; leave it null to use BackgroundJobsOptions.DefaultJobTimeout");

        var registration = new JobRegistration(name, typeof(TArgs), options, async (services, args, context) =>
        {
            var typedArgs = args.Deserialize<TArgs>(JobJson.Options)!;
            var job = ActivatorUtilities.GetServiceOrCreateInstance<TJob>(services);
            await job.ExecuteAsync(typedArgs, context);
        });
        if (!_jobs.TryAdd(name, registration)) throw new ArgumentException($"job '{name}' is already registered; register each name once, or pick another name for the second job");
        lock (_jobs) _names = _jobs.Keys.Order(StringComparer.Ordinal).ToArray();
        if (options.MaxConcurrency is { } max)
            lock (_jobs) _concurrencyLimits = new Dictionary<string, int>(_concurrencyLimits) { [name] = max };
        return this;
    }

    internal JobRegistration? Find(string name) => _jobs.GetValueOrDefault(name);

    /// <summary>
    /// The options <paramref name="name"/> was registered with (timeout, concurrency, circuit breaker, args version), or
    /// null when the name is not registered. For display: changing them after the host starts is not supported.
    /// </summary>
    public JobRegistrationOptions? GetOptions(string name) => Find(name)?.Options;

    internal IEnumerable<JobRegistration> All => _jobs.Values;

    /// <summary>
    /// Job name → <see cref="JobRegistrationOptions.MaxConcurrency"/>, for the names that set one.
    /// Rebuilt on registration, so claims read it without allocating.
    /// </summary>
    internal IReadOnlyDictionary<string, int> ConcurrencyLimits => _concurrencyLimits;

    /// <summary>
    /// The registered job names, ordered; what this node claims (see ClaimRequest.Names). Rebuilt on registration.
    /// </summary>
    internal IReadOnlyCollection<string> Names => _names;

    /// <summary>
    /// Registered job names with their argument types, ordered by name.
    /// </summary>
    public IReadOnlyList<(string Name, Type ArgsType)> Registrations =>
        _jobs.Values.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => (r.Name, r.ArgsType)).ToList();
}

internal sealed record JobRegistration(
    string Name,
    Type ArgsType,
    JobRegistrationOptions Options,
    Func<IServiceProvider, JsonElement, JobContext, Task> InvokeCurrent)
{
    /// <summary>
    /// Brings args stored as <paramref name="version"/> to the registered version, then runs the job.
    /// </summary>
    public Task Invoke(IServiceProvider services, JsonElement args, int version, JobContext context)
    {
        if (version > Options.ArgsVersion)
            throw new InvalidOperationException(
                $"args version {version} of job '{Name}' is newer than the registered version {Options.ArgsVersion}; newer code enqueued it: " +
                "run that version on every node consuming the queue before enqueuing with it");
        if (version == Options.ArgsVersion) return InvokeCurrent(services, args, context);

        var node = JsonNode.Parse(args.GetRawText()) as JsonObject
                   ?? throw new InvalidOperationException(
                       $"args of job '{Name}' (version {version}) are not a JSON object, so the UpgradeArgs steps to version {Options.ArgsVersion} cannot run; " +
                       "enqueue a new job with args in the current shape");
        for (var v = version; v < Options.ArgsVersion; v++) node = Options.Upgrades[v](node);
        return InvokeCurrent(services, JsonSerializer.SerializeToElement(node, JobJson.Options), context);
    }

    /// <summary>
    /// Enqueuing under a registered name must pass that registration's args type (or raw JSON in the current shape).
    /// </summary>
    public void CheckArgs(object? args)
    {
        if (args is null or JsonElement or JsonNode || ArgsType.IsInstanceOfType(args)) return;
        throw new ArgumentException(
            $"job '{Name}' takes {ArgsType.Name} args, not {args.GetType().Name}; pass a {ArgsType.Name} (or a JsonElement/JsonNode of its shape)");
    }
}

/// <summary>
/// Serializer options used for job args, so callers that build args by hand serialize them the same way.
/// </summary>
public static class JobJson
{
    /// <summary>
    /// <see cref="JsonSerializerDefaults.Web"/>: camelCase names, case-insensitive reads. Do not mutate.
    /// </summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
