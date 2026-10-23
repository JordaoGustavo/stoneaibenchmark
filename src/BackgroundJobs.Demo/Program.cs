using BackgroundJobs;
using BackgroundJobs.Demo.Endpoints;
using BackgroundJobs.Demo.Events;
using BackgroundJobs.Demo.Jobs;
using BackgroundJobs.Demo.Telemetry;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var port = Env("PORT", "8080");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
// Graceful stop waits for running jobs without a time limit.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);

builder.Services.AddSingleton<EventLog>();
// Created eagerly so it is listening before the first job runs.
var telemetry = new TelemetryRecorder();
builder.Services.AddSingleton(telemetry);
// Off unless set: lets tests see the per-type breaker in the health report.
var failBreaker = int.TryParse(Env("FAIL_BREAKER_THRESHOLD", ""), out var threshold)
    ? new CircuitBreakerOptions(threshold, TimeSpan.FromSeconds(int.Parse(Env("FAIL_BREAKER_SECONDS", "30"))))
    : null;
// Which "greet" code this process runs: 1 = before its args changed, 2 = today.
var greetVersion = int.Parse(Env("GREET_ARGS_VERSION", "2"));
var jobs = builder.Services.AddBackgroundJobs(
    o =>
    {
        o.WorkerCount = int.Parse(Env("WORKER_COUNT", "4"));
        o.NodeId = Env("NODE_ID", Environment.MachineName);
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
    registry => registry
        .Register<SucceedAfterJob, SucceedAfterArgs>(SucceedAfterJob.Name)
        .Register<FailNTimesJob, FailNTimesArgs>(FailNTimesJob.Name, o => o.CircuitBreaker = failBreaker)
        .Register<LogMessageJob, LogMessageArgs>(LogMessageJob.Name)
        .Register<SleepJob, SleepArgs>(SleepJob.Name, o => o.Timeout = Milliseconds("SLEEP_TIMEOUT_MS"))
        .RegisterGreet(greetVersion));
if (Env("STORAGE", "postgres") == "memory") jobs.UseInMemoryStorage();
else jobs.UsePostgres(ToConnectionString(Env("DATABASE_URL", "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs")));

var app = builder.Build();
app.MapBackgroundJobsApi();
app.MapBackgroundJobsDashboard();
app.MapDemoEndpoints();
app.Run();

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : fallback;

static TimeSpan? Milliseconds(string name) => int.TryParse(Env(name, ""), out var ms) ? TimeSpan.FromMilliseconds(ms) : null;

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
