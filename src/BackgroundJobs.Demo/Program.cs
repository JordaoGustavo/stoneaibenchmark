using BackgroundJobs;
using BackgroundJobs.Demo.Endpoints;
using BackgroundJobs.Demo.Events;
using BackgroundJobs.Demo.Jobs;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var port = Env("PORT", "8080");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
// Graceful stop waits for running jobs without a time limit.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);

builder.Services.AddSingleton<EventLog>();
builder.Services.AddBackgroundJobs(
    o =>
    {
        o.ConnectionString = ToConnectionString(Env("DATABASE_URL", "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs"));
        o.WorkerCount = int.Parse(Env("WORKER_COUNT", "4"));
        o.NodeId = Env("NODE_ID", Environment.MachineName);
    },
    jobs => jobs
        .Register<SucceedAfterJob, SucceedAfterArgs>(SucceedAfterJob.Name)
        .Register<FailNTimesJob, FailNTimesArgs>(FailNTimesJob.Name)
        .Register<LogMessageJob, LogMessageArgs>(LogMessageJob.Name));

var app = builder.Build();
app.MapBackgroundJobsApi();
app.MapBackgroundJobsDashboard();
app.MapDemoEndpoints();
app.Run();

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : fallback;

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
