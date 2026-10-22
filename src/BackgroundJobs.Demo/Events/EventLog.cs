namespace BackgroundJobs.Demo.Events;

public sealed record DemoEvent(string Type, string JobId, int Attempt, DateTime At, object? Detail);

/// In-memory record of what job code actually ran; independent of the library's history. Lost on restart.
public sealed class EventLog(TimeProvider time)
{
    private readonly List<DemoEvent> _events = [];
    private readonly Lock _lock = new();

    public void Started(JobContext ctx) => Add("started", ctx, null);
    public void Finished(JobContext ctx, bool ok) => Add("finished", ctx, new { outcome = ok ? "ok" : "error" });
    public void Message(JobContext ctx, string message) => Add("message", ctx, new { message });

    public IReadOnlyList<DemoEvent> All()
    {
        lock (_lock) return _events.ToList();
    }

    private void Add(string type, JobContext ctx, object? detail)
    {
        lock (_lock) _events.Add(new DemoEvent(type, ctx.JobId.ToString(), ctx.Attempt, time.GetUtcNow().UtcDateTime, detail));
    }
}
