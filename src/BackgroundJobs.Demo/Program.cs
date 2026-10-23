using BackgroundJobs;
using BackgroundJobs.Demo.Endpoints;
using BackgroundJobs.Demo.Events;
using BackgroundJobs.Demo.Jobs;
using BackgroundJobs.Demo.Telemetry;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

var port = Env("PORT", "8080");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
// Graceful stop waits for running jobs without a time limit.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);

builder.Services.AddSingleton<EventLog>();
// Created eagerly so it is listening before the first job runs.
var telemetry = new TelemetryRecorder();
builder.Services.AddSingleton(telemetry);
// Real export on top of the in-memory recorder; the exporter reads the standard OTEL_EXPORTER_OTLP_* variables.
if (Env("OTEL_EXPORTER_OTLP_ENDPOINT", "") is { Length: > 0 })
{
    builder.Services.AddOpenTelemetry()
        .ConfigureResource(r => r.AddService(Env("OTEL_SERVICE_NAME", "backgroundjobs-demo")))
        .WithTracing(t => t.AddSource(BackgroundJobsTelemetry.Name).AddOtlpExporter())
        .WithMetrics(m => m.AddMeter(BackgroundJobsTelemetry.Name).AddOtlpExporter());
}
// Off unless set: lets tests see the per-type breaker in the health report.
var failBreaker = int.TryParse(Env("FAIL_BREAKER_THRESHOLD", ""), out var threshold)
    ? new CircuitBreakerOptions(threshold, TimeSpan.FromSeconds(int.Parse(Env("FAIL_BREAKER_SECONDS", "30"))))
    : null;
// Which "greet" code this process runs: 1 = before its args changed, 2 = today.
var greetVersion = int.Parse(Env("GREET_ARGS_VERSION", "2"));
// Lets tests run nodes whose clocks disagree. Registered before AddBackgroundJobs, which keeps it.
if (int.TryParse(Env("CLOCK_OFFSET_MS", ""), out var clockOffset))
    builder.Services.AddSingleton<TimeProvider>(new OffsetTimeProvider(TimeSpan.FromMilliseconds(clockOffset)));
var jobs = builder.Services.AddBackgroundJobs(
    o =>
    {
        o.WorkerCount = int.Parse(Env("WORKER_COUNT", "4"));
        o.FifoClaimEvery = Env("FIFO_CLAIM_EVERY", "") switch { "" => o.FifoClaimEvery, "off" => null, var n => int.Parse(n) };
        // "name=workers,name=workers"; unset consumes only the default queue with WORKER_COUNT workers.
        foreach (var queue in Env("QUEUES", "").Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = queue.Split('=');
            if (parts.Length != 2 || !int.TryParse(parts[1], out var workers))
                throw new ArgumentException($"QUEUES entry '{queue}' must be name=workers");
            o.Queues[parts[0]] = workers;
        }
        o.NodeId = Env("NODE_ID", o.NodeId);
        // Global retry backoff defaults; unset keeps the library's (fixed delay).
        if (Env("RETRY_BACKOFF", "") == "exponential") o.DefaultRetryBackoff = RetryBackoff.Exponential;
        if (Env("RETRY_BACKOFF_MULTIPLIER", "") is { Length: > 0 } multiplier)
            o.DefaultRetryBackoffMultiplier = double.Parse(multiplier, System.Globalization.CultureInfo.InvariantCulture);
        if (int.TryParse(Env("MAX_RETRY_DELAY_MS", ""), out var maxRetryDelayMs)) o.DefaultMaxRetryDelay = TimeSpan.FromMilliseconds(maxRetryDelayMs);
        o.QueueMonitorInterval = TimeSpan.FromMilliseconds(int.Parse(Env("QUEUE_MONITOR_MS", "5000")));
        // Required by the library. "unset" leaves it unset so tests can see startup refuse it.
        o.FinishedJobRetention = Env("RETENTION_SECONDS", "604800") switch
        {
            "unset" => null,
            "forever" => Timeout.InfiniteTimeSpan,
            var seconds => TimeSpan.FromSeconds(double.Parse(seconds, System.Globalization.CultureInfo.InvariantCulture)),
        };
        o.RetentionSweepInterval = TimeSpan.FromMilliseconds(int.Parse(Env("RETENTION_SWEEP_MS", "60000")));
        o.RetentionBatchSize = int.Parse(Env("RETENTION_BATCH", "1000"));
        o.DefaultJobTimeout = Milliseconds("JOB_TIMEOUT_MS") ?? Timeout.InfiniteTimeSpan;
        o.TimeoutGracePeriod = Milliseconds("TIMEOUT_GRACE_MS") ?? o.TimeoutGracePeriod;
        o.MaxAbandonedJobs = int.Parse(Env("MAX_ABANDONED_JOBS", o.MaxAbandonedJobs.ToString()));
        o.AbandonedJobAlertAfter = Milliseconds("ABANDONED_ALERT_MS") ?? o.AbandonedJobAlertAfter;
        o.ShutdownTimeout = Milliseconds("SHUTDOWN_TIMEOUT_MS") ?? Timeout.InfiniteTimeSpan;
        o.LeaseDuration = Milliseconds("LEASE_MS") ?? o.LeaseDuration;
        o.StalledQueueThreshold = TimeSpan.FromSeconds(double.Parse(Env("STALLED_QUEUE_SECONDS", "60"), System.Globalization.CultureInfo.InvariantCulture));
    },
    registry =>
    {
        // UNREGISTERED_JOBS (comma-separated job names) leaves those out, like a node of an older deploy.
        var skip = Env("UNREGISTERED_JOBS", "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (!skip.Contains(SucceedAfterJob.Name))
            registry.Register<SucceedAfterJob, SucceedAfterArgs>(SucceedAfterJob.Name, o => o.MaxConcurrency = Int("SUCCEED_AFTER_MAX_CONCURRENCY"));
        if (!skip.Contains(FailNTimesJob.Name))
            registry.Register<FailNTimesJob, FailNTimesArgs>(FailNTimesJob.Name, o => o.CircuitBreaker = failBreaker);
        if (!skip.Contains(LogMessageJob.Name)) registry.Register<LogMessageJob, LogMessageArgs>(LogMessageJob.Name);
        if (!skip.Contains(SleepJob.Name)) registry.Register<SleepJob, SleepArgs>(SleepJob.Name, o => o.Timeout = Milliseconds("SLEEP_TIMEOUT_MS"));
        if (!skip.Contains(GreetJob.Name)) registry.RegisterGreet(greetVersion);
    });
// Declared in code, upserted at every start.
if (Env("RECURRING_HEARTBEAT_CRON", "") is { Length: > 0 } heartbeatCron)
    jobs.AddRecurringJob("heartbeat", LogMessageJob.Name, heartbeatCron, new LogMessageArgs("heartbeat"));
var connectionString = Env("STORAGE", "postgres") == "memory"
    ? null
    : ToConnectionString(Env("DATABASE_URL", "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs"));
if (connectionString == null) jobs.UseInMemoryStorage();
else jobs.UsePostgres(connectionString);
builder.Services.AddSingleton(new DemoDatabase(connectionString));

var app = builder.Build();
app.MapBackgroundJobsApi();
app.MapBackgroundJobsDashboard();
app.MapDemoEndpoints();
app.Run();

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : fallback;

static TimeSpan? Milliseconds(string name) => Int(name) is { } ms ? TimeSpan.FromMilliseconds(ms) : null;

static int? Int(string name) => int.TryParse(Env(name, ""), out var value) ? value : null;

// Accepts either an Npgsql connection string or a postgres://user:pass@host:port/db URL.
static string ToConnectionString(string value)
{
    if (!value.StartsWith("postgres://") && !value.StartsWith("postgresql://")) return value;
    var uri = new Uri(value);
    var userInfo = uri.UserInfo.Split(':', 2);
    return new NpgsqlConnectionStringBuilder
    {
        Host = uri.Host,
        Port = uri.Port > 0 ? uri.Port : 5432,
        Username = Uri.UnescapeDataString(userInfo[0]),
        Password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : null,
        Database = uri.AbsolutePath.Trim('/'),
    }.ConnectionString;
}

/// The system clock shifted by a fixed offset; timers and timestamps are untouched.
sealed class OffsetTimeProvider(TimeSpan offset) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => System.GetUtcNow() + offset;
}

static partial class Registrations
{
    public static JobRegistry RegisterGreet(this JobRegistry jobs, int version) => version == 1
        ? jobs.Register<GreetV1Job, GreetV1Args>(GreetJob.Name)
        : jobs.Register<GreetJob, GreetArgs>(GreetJob.Name, o =>
        {
            o.ArgsVersion = 2;
            o.UpgradeArgs(1, GreetJob.UpgradeFromV1);
        });
}
