// A web app that enqueues jobs from its own endpoint and exposes the management API and the dashboard.
using BackgroundJobs;

// Its own queue, so other apps sharing the database (which would fail "send-email" as an unknown job) never claim it.
const string Queue = "sample-aspnet";

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddBackgroundJobs(o => { o.FinishedJobRetention = TimeSpan.FromDays(7); o.Queues = new() { [Queue] = 4 }; }, registerJobs: jobs => jobs.Register<SendEmailJob, SendEmailArgs>("send-email"))
    .UsePostgres(Environment.GetEnvironmentVariable("DATABASE_URL")
                 ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var app = builder.Build();
app.MapBackgroundJobsApi();       // JSON management API under /api/jobs and /api/recurring, plus /health
app.MapBackgroundJobsDashboard(); // HTML dashboard at /jobs; protect it with RequireAuthorization() in real apps

// Your own endpoint: enqueue and return the id right away; the job runs in the background.
app.MapPost("/signup", async (string email, IJobClient client) =>
{
    var id = await client.EnqueueAsync("send-email", new SendEmailArgs(email, "Welcome!"), new JobOptions { Queue = Queue });
    return Results.Accepted($"/api/jobs/{id}", new { id });
});

app.Run(); // try: curl -X POST "http://localhost:5000/signup?email=a@b.c", then GET /api/jobs/{id} or open http://localhost:5000/jobs

public sealed record SendEmailArgs(string To, string Subject);

public sealed class SendEmailJob(ILogger<SendEmailJob> logger) : IJob<SendEmailArgs>
{
    public async Task ExecuteAsync(SendEmailArgs args, JobContext context)
    {
        await Task.Delay(200, context.CancellationToken); // pretend to call an email provider
        logger.LogInformation("sent '{Subject}' to {To} (job {JobId})", args.Subject, args.To, context.JobId);
    }
}
