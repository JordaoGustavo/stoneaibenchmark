using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BackgroundJobs;

public static class DashboardEndpoints
{
    private const int PageSize = 50;
    private const int ScanBatch = 1000;
    private const int MaxScannedPerPage = 10_000;
    private const int RefreshSeconds = 5;

    private static readonly JsonSerializerOptions PrettyJson = new() { WriteIndented = true };

    /// Maps a server-rendered dashboard under <paramref name="prefix"/>. Opt-in.
    /// Pages are rendered from IJobClient on the server: the browser runs no script and makes no API calls;
    /// actions are same-origin form posts followed by a redirect. Protect the route group like any admin surface,
    /// e.g. <c>app.MapBackgroundJobsDashboard().RequireAuthorization("admin")</c>.
    public static RouteGroupBuilder MapBackgroundJobsDashboard(
        this IEndpointRouteBuilder app, string prefix = "/jobs", Action<DashboardOptions>? configure = null)
    {
        var options = new DashboardOptions();
        configure?.Invoke(options);
        var group = app.MapGroup(prefix).ExcludeFromDescription();
        var basePath = prefix.TrimEnd('/');
        group.AddEndpointFilter((ctx, next) =>
        {
            ctx.HttpContext.Items[OptionsKey] = options;
            return next(ctx);
        });

        // Viewer preferences live in cookies so every page can render times and theme without script.
        group.MapPost("/preferences", async (HttpContext http, CancellationToken ct) =>
        {
            if (IsCrossSite(http)) return Page(http, 403, "Forbidden", Alert("Cross-site request rejected."), basePath);
            var form = await http.Request.ReadFormAsync(ct);
            var cookie = new CookieOptions { Path = basePath.Length == 0 ? "/" : basePath, HttpOnly = true, SameSite = SameSiteMode.Lax, MaxAge = TimeSpan.FromDays(365) };
            var theme = form["theme"].ToString();
            if (theme is "light" or "dark" or "system") http.Response.Cookies.Append(ThemeCookie, theme, cookie);
            if (form["tz"].ToString() is { Length: > 0 } tz && TimeZoneInfo.TryFindSystemTimeZoneById(tz, out _)) http.Response.Cookies.Append(ZoneCookie, tz, cookie);
            var back = form["return"].ToString();
            // Only redirect within the dashboard; anything else could be an open redirect.
            var safe = back.StartsWith(basePath + "/", StringComparison.Ordinal) || back == basePath;
            return Results.Redirect(safe && !back.StartsWith("//") && back.Length > 0 ? back : (basePath.Length == 0 ? "/" : basePath));
        });

        group.MapGet("/", async (HttpContext http, IJobClient client, string? state, string? after, string? refresh, string? msg, CancellationToken ct) =>
        {
            JobState? filter = null;
            if (!string.IsNullOrEmpty(state))
            {
                if (!Enum.TryParse<JobState>(state, ignoreCase: true, out var parsed)) return Page(http, 400, "Invalid filter", Alert($"Unknown state \"{state}\"."), basePath);
                filter = parsed;
            }
            Guid? cursor = Guid.TryParse(after, out var a) ? a : null;
            var autoRefresh = refresh != "off";

            var (items, next) = await ScanAsync(client, filter, cursor, ct);
            var tz = Zone(http);
            var now = DateTime.UtcNow;
            var body = new StringBuilder();
            body.Append("<div class=\"head\"><div><h1>Jobs</h1><p class=\"muted\">")
                .Append(filter is null ? "All jobs" : $"Jobs in state <b>{filter.Value.ToWire()}</b>")
                .Append(", oldest first.</p></div>")
                .Append("<div class=\"actions\">").Append(RefreshToggle(basePath, filter, cursor, autoRefresh))
                .Append("<a class=\"btn\" href=\"").Append(basePath).Append("/new\">Trigger job</a></div>")
                .Append("</div>");
            if (!string.IsNullOrEmpty(msg)) body.Append(Alert(msg, info: true));

            body.Append("<nav class=\"tabs\">").Append(Tab(basePath, null, filter, autoRefresh));
            foreach (var s in Enum.GetValues<JobState>()) body.Append(Tab(basePath, s, filter, autoRefresh));
            body.Append("</nav>");

            body.Append("<div class=\"card\"><table><thead><tr><th>Name</th><th>State</th><th class=\"num\">Attempts</th><th>Created</th><th>Updated</th><th>Id</th></tr></thead><tbody>");
            if (items.Count == 0) body.Append("<tr><td colspan=\"6\" class=\"empty\">No jobs found.</td></tr>");
            foreach (var j in items)
            {
                var href = $"{basePath}/{j.Id}";
                body.Append("<tr><td><a href=\"").Append(href).Append("\">").Append(E(j.Name)).Append("</a></td><td>")
                    .Append(Badge(j.State)).Append("</td><td class=\"num\">").Append(j.Attempts)
                    .Append("</td><td class=\"nowrap\">").Append(Local(j.CreatedAt, tz, now))
                    .Append("</td><td class=\"nowrap\">").Append(Local(j.UpdatedAt, tz, now))
                    .Append("</td><td class=\"mono muted id\">").Append(j.Id.ToString()[..8]).Append("</td></tr>");
            }
            body.Append("</tbody></table></div><div class=\"pager\">");
            if (cursor != null) body.Append("<a class=\"btn outline\" href=\"").Append(ListUrl(basePath, filter, null, autoRefresh)).Append("\">First page</a>");
            if (next != null) body.Append("<a class=\"btn outline\" href=\"").Append(ListUrl(basePath, filter, next, autoRefresh)).Append("\">Next page</a>");
            body.Append("</div>");

            return Page(http, 200, "Jobs", body.ToString(), basePath, autoRefresh);
        });

        group.MapGet("/new", (HttpContext http, JobRegistry registry) =>
        {
            var body = new StringBuilder();
            body.Append("<a class=\"back\" href=\"").Append(basePath).Append("\">&larr; All jobs</a>")
                .Append("<div class=\"head\"><div><h1>Trigger a job</h1><p class=\"muted\">Pick a registered job type to enqueue.</p></div></div>");
            var jobs = registry.Registrations;
            if (jobs.Count == 0) body.Append("<div class=\"card pad empty\">No job types are registered.</div>");
            body.Append("<div class=\"picker\">");
            foreach (var (name, argsType) in jobs)
                body.Append("<a class=\"card pad pick\" href=\"").Append(basePath).Append("/new/").Append(Uri.EscapeDataString(name)).Append("\"><b>")
                    .Append(E(name)).Append("</b><span class=\"mono muted\">").Append(E(argsType.Name)).Append("</span></a>");
            body.Append("</div>");
            return Page(http, 200, "Trigger a job", body.ToString(), basePath);
        });

        group.MapGet("/new/{name}", (HttpContext http, JobRegistry registry, string name) =>
            registry.Registrations.Any(r => r.Name == name)
                ? TriggerPage(http, basePath, registry, name, null, [])
                : Page(http, 404, "Not found", BackToTrigger(basePath) + Alert($"No job type named \"{name}\" is registered."), basePath));

        group.MapPost("/new/{name}", async (HttpContext http, JobRegistry registry, IJobClient client, string name, CancellationToken ct) =>
        {
            if (IsCrossSite(http)) return Page(http, 403, "Forbidden", Alert("Cross-site request rejected."), basePath);
            var reg = registry.Registrations.FirstOrDefault(r => r.Name == name);
            if (reg.Name is null) return Page(http, 404, "Not found", BackToTrigger(basePath) + Alert($"No job type named \"{name}\" is registered."), basePath);

            var form = await http.Request.ReadFormAsync(ct);
            var errors = new List<string>();
            var args = DashboardForms.Parse(reg.ArgsType, form, errors);
            var options = ParseOptions(form, errors);
            if (errors.Count > 0) return TriggerPage(http, basePath, registry, name, form, errors);

            Guid id;
            try { id = await client.EnqueueAsync(name, args, options, ct); }
            catch (ArgumentException e) { return TriggerPage(http, basePath, registry, name, form, [e.Message]); }
            return Results.Redirect($"{basePath}/{id}?msg={Uri.EscapeDataString("Job enqueued.")}");
        });

        group.MapGet("/{id}", async (HttpContext http, IJobClient client, TimeProvider time, string id, string? msg, CancellationToken ct) =>
        {
            if (!Guid.TryParse(id, out var jobId)) return NotFound(http, basePath, id);
            Job job;
            try { job = await client.GetAsync(jobId, ct); }
            catch (JobNotFoundException) { return NotFound(http, basePath, id); }

            var now = time.GetUtcNow().UtcDateTime;
            var tz = Zone(http);
            var terminal = JobStateWire.Finished.Contains(job.State);
            var runs = Runs(job);
            var body = new StringBuilder();
            body.Append("<a class=\"back\" href=\"").Append(basePath).Append("\">&larr; All jobs</a>");
            body.Append("<div class=\"head\"><div><h1>").Append(E(job.Name)).Append(' ').Append(Badge(job.State))
                .Append("</h1><p class=\"mono muted\">").Append(job.Id).Append("</p></div><div class=\"actions\">");
            if (job.State.CanRequeue())
                body.Append(ActionForm($"{basePath}/{job.Id}/requeue", "Requeue", "btn"));
            if (job.State.CanDelete())
                body.Append(ActionForm($"{basePath}/{job.Id}/delete", "Delete", "btn destructive"));
            if (job.State.CanCancel())
                body.Append(ActionForm($"{basePath}/{job.Id}/cancel", "Cancel", "btn destructive"));
            body.Append("</div></div>");
            if (!string.IsNullOrEmpty(msg)) body.Append(Alert(msg, info: true));

            // The answer to "why is this failed?" goes first, not at the bottom of a timeline.
            var last = job.AttemptHistory.LastOrDefault();
            if (job.State == JobState.Failed && last?.Error != null)
                body.Append("<div class=\"callout\"><b>Failed after ").Append(job.Attempts - job.RefundedAttempts).Append(job.Attempts - job.RefundedAttempts == 1 ? " attempt" : " attempts")
                    .Append("</b> <span class=\"muted\">").Append(Ago(last.FinishedAt ?? last.StartedAt, now, tz)).Append("</span><pre>")
                    .Append(E(last.Error)).Append("</pre></div>");

            body.Append("<div class=\"card meta\"><dl>")
                .Append(Meta("Attempts", job.RefundedAttempts == 0
                    ? $"{job.Attempts - job.RefundedAttempts} of {job.MaxAttempts}"
                    : $"{job.Attempts - job.RefundedAttempts} of {job.MaxAttempts} (+{job.RefundedAttempts} interrupted by shutdown)"))
                .Append(Meta("Queue", E(job.Queue)))
                .Append(Meta("Priority", job.Priority.ToString()))
                .Append(Meta("Retry delay", Duration(job.RetryDelay)))
                .Append(Meta("Runs", runs.Count.ToString()))
                .Append(Meta("Created", Local(job.CreatedAt, tz, now)))
                .Append(Meta("Updated", Local(job.UpdatedAt, tz, now)));
            if (job.ScheduledAt is { } at) body.Append(Meta("Scheduled for", Local(at, tz, now)));
            body.Append("</dl></div>");

            body.Append("<div class=\"split\"><section><h2>Timeline</h2>");
            for (var i = runs.Count - 1; i >= 0; i--)
                body.Append(RunCard(runs[i], i + 1, open: i == runs.Count - 1, now, tz));
            body.Append("</section><aside><h2>Arguments</h2>").Append(ArgsCard(job.Args)).Append("</aside></div>");

            return Page(http, 200, job.Name, body.ToString(), basePath, autoRefresh: !terminal);
        });

        group.MapPost("/{id}/delete", (HttpContext http, IJobClient client, string id, CancellationToken ct) =>
            RunAction(http, basePath, id, jobId => client.DeleteAsync(jobId, ct), "Job deleted."));

        group.MapPost("/{id}/requeue", (HttpContext http, IJobClient client, string id, CancellationToken ct) =>
            RunAction(http, basePath, id, jobId => client.RequeueAsync(jobId, ct), "Job requeued."));

        group.MapPost("/{id}/cancel", (HttpContext http, IJobClient client, string id, CancellationToken ct) =>
            RunAction(http, basePath, id, jobId => client.CancelAsync(jobId, ct), "Cancel requested."));

        return group;
    }

    /// Walks the id-ordered list until a page of matches is filled; the store has no state index to filter on.
    private static async Task<(List<JobSummary> Items, Guid? Next)> ScanAsync(IJobClient client, JobState? filter, Guid? after, CancellationToken ct)
    {
        var items = new List<JobSummary>();
        var scanned = 0;
        var cursor = after;
        while (scanned < MaxScannedPerPage)
        {
            var batch = await client.ListAsync(cursor, ScanBatch, ct);
            foreach (var j in batch)
            {
                cursor = j.Id;
                if (filter is null || j.State == filter) items.Add(j);
                if (items.Count == PageSize) return (items, j.Id);
            }
            scanned += batch.Count;
            if (batch.Count < ScanBatch) return (items, null);
        }
        return (items, cursor);
    }

    private static async Task<IResult> RunAction(HttpContext http, string basePath, string id, Func<Guid, Task> action, string success)
    {
        if (IsCrossSite(http)) return Page(http, 403, "Forbidden", Alert("Cross-site request rejected."), basePath);
        if (!Guid.TryParse(id, out var jobId)) return NotFound(http, basePath, id);

        string msg;
        try { await action(jobId); msg = success; }
        catch (JobNotFoundException) { return NotFound(http, basePath, id); }
        catch (InvalidJobTransitionException e) { msg = e.Message; }
        return Results.Redirect($"{basePath}/{jobId}?msg={Uri.EscapeDataString(msg)}");
    }

    // Form posts carry no token; reject anything a browser marks as cross-site.
    private static bool IsCrossSite(HttpContext http) =>
        http.Request.Headers["Sec-Fetch-Site"].ToString() is "cross-site" or "same-site";

    private static string BackToTrigger(string basePath) => $"<a class=\"back\" href=\"{basePath}/new\">&larr; All job types</a>";

    private static IResult TriggerPage(HttpContext http, string basePath, JobRegistry registry, string name, IFormCollection? posted, List<string> errors)
    {
        var argsType = registry.Registrations.First(r => r.Name == name).ArgsType;
        string Posted(string key) => E(posted?[key].LastOrDefault() ?? "");
        var body = new StringBuilder(BackToTrigger(basePath));
        body.Append("<div class=\"head\"><div><h1>").Append(E(name)).Append("</h1><p class=\"muted\">Arguments are validated against <span class=\"mono\">")
            .Append(E(argsType.Name)).Append("</span> before the job is enqueued.</p></div></div>");
        if (errors.Count > 0)
            body.Append("<div class=\"alert\"><b>Could not enqueue</b><ul>").Append(string.Concat(errors.Select(e => $"<li>{E(e)}</li>"))).Append("</ul></div>");

        body.Append("<form class=\"card pad form\" method=\"post\" action=\"").Append(basePath).Append("/new/").Append(Uri.EscapeDataString(name)).Append("\">")
            .Append("<fieldset><legend>Arguments</legend>").Append(DashboardForms.Render(argsType, posted)).Append("</fieldset>")
            .Append("<details class=\"opts\"><summary>Options</summary><p class=\"muted\">Leave blank to use the server defaults.</p><div class=\"row\">")
            .Append("<div class=\"field\"><label for=\"o-runAt\">Run at <span class=\"muted\">(UTC)</span></label><input type=\"datetime-local\" step=\"1\" id=\"o-runAt\" name=\"opt.runAt\" value=\"").Append(Posted("opt.runAt")).Append("\"></div>")
            .Append("<div class=\"field\"><label for=\"o-max\">Max attempts</label><input type=\"number\" min=\"1\" step=\"1\" id=\"o-max\" name=\"opt.maxAttempts\" value=\"").Append(Posted("opt.maxAttempts")).Append("\"></div>")
            .Append("<div class=\"field\"><label for=\"o-delay\">Retry delay (ms)</label><input type=\"number\" min=\"0\" step=\"1\" id=\"o-delay\" name=\"opt.retryDelayMs\" value=\"").Append(Posted("opt.retryDelayMs")).Append("\"></div>")
            .Append("</div></details><div class=\"actions end\"><a class=\"btn outline\" href=\"").Append(basePath).Append("/new\">Cancel</a>")
            .Append("<button class=\"btn\" type=\"submit\">Enqueue job</button></div></form>");
        return Page(http, errors.Count > 0 ? 400 : 200, name, body.ToString(), basePath);
    }

    private static JobOptions? ParseOptions(IFormCollection form, List<string> errors)
    {
        string? Get(string key) => form[key].LastOrDefault() is { Length: > 0 } v ? v.Trim() : null;
        DateTimeOffset? runAt = null;
        int? maxAttempts = null;
        TimeSpan? retryDelay = null;
        if (Get("opt.runAt") is { } r)
        {
            if (DateTime.TryParse(r, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var d)) runAt = new DateTimeOffset(d, TimeSpan.Zero);
            else errors.Add("Run at: a date and time expected.");
        }
        if (Get("opt.maxAttempts") is { } m)
        {
            if (int.TryParse(m, out var n) && n >= 1) maxAttempts = n;
            else errors.Add("Max attempts must be a whole number of at least 1.");
        }
        if (Get("opt.retryDelayMs") is { } ms)
        {
            if (int.TryParse(ms, out var n) && n >= 0) retryDelay = TimeSpan.FromMilliseconds(n);
            else errors.Add("Retry delay must be a whole number of milliseconds.");
        }
        return runAt is null && maxAttempts is null && retryDelay is null ? null : new JobOptions { RunAt = runAt, MaxAttempts = maxAttempts, RetryDelay = retryDelay };
    }

    private sealed record Run(List<JobHistoryEntry> Events, List<JobAttempt> Attempts);

    /// Splits history into runs: creation and every user requeue start a new one. Attempt numbers restart per run,
    /// so showing attempts grouped by run is what keeps "#1, #2, #1, #2" readable.
    private static List<Run> Runs(Job job)
    {
        var runs = new List<Run>();
        foreach (var h in job.History)
        {
            if (runs.Count == 0 || h.Reason is Reasons.RequeuedByUser) runs.Add(new Run([], []));
            runs[^1].Events.Add(h);
        }
        if (runs.Count == 0) runs.Add(new Run([], []));
        var starts = runs.Select(r => r.Events.Count > 0 ? r.Events[0].At : DateTime.MinValue).ToList();
        foreach (var a in job.AttemptHistory.OrderBy(a => a.StartedAt))
        {
            var i = starts.FindLastIndex(s => s <= a.StartedAt);
            runs[Math.Max(i, 0)].Attempts.Add(a);
        }
        return runs;
    }

    private static string RunCard(Run run, int number, bool open, DateTime now, TimeZoneInfo tz)
    {
        var first = run.Events.FirstOrDefault();
        var final = run.Events.LastOrDefault();
        var sb = new StringBuilder();
        sb.Append("<details class=\"card run\"").Append(open ? " open" : "").Append("><summary><b>Run ").Append(number).Append("</b>");
        if (final != null) sb.Append(Badge(final.State));
        sb.Append("<span class=\"muted\">").Append(first == null ? "" : $"{Humanize(first.Reason)} · {Ago(first.At, now, tz)}")
          .Append(" · ").Append(run.Attempts.Count).Append(run.Attempts.Count == 1 ? " attempt" : " attempts").Append("</span></summary>");

        // A plain, monochrome event log: time | event | state. Colour is reserved for the one thing that needs it, errors.
        // Each claim row becomes the attempt it started, so outcome and error sit where they happened.
        sb.Append("<table class=\"log\"><tbody>");
        var pending = new Queue<JobAttempt>(run.Attempts);
        foreach (var h in run.Events)
        {
            JobAttempt? attempt = h.Reason == Reasons.Claimed && pending.TryDequeue(out var a) ? a : null;
            sb.Append(LogRow(h.At, tz, attempt is null ? E(Humanize(h.Reason)) : AttemptLabel(attempt), h.State.ToWire(), attempt?.Error));
        }
        while (pending.TryDequeue(out var a)) sb.Append(LogRow(a.StartedAt, tz, AttemptLabel(a), "processing", a.Error));
        return sb.Append("</tbody></table></details>").ToString();
    }

    private static string LogRow(DateTime at, TimeZoneInfo tz, string label, string state, string? error)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(at, DateTimeKind.Utc), tz);
        var sb = new StringBuilder("<tr><td class=\"mono muted t\"><time datetime=\"").Append($"{at:O}").Append("\" title=\"")
            .Append($"{local:yyyy-MM-dd HH:mm:ss.fff} {E(tz.Id)}").Append("\">").Append($"{local:HH:mm:ss.fff}").Append("</time></td><td>").Append(label);
        if (error != null) sb.Append("<pre class=\"error\">").Append(E(error)).Append("</pre>");
        return sb.Append("</td><td class=\"muted s\">").Append(state).Append("</td></tr>").ToString();
    }

    private static string AttemptLabel(JobAttempt a)
    {
        var outcome = a.Outcome?.ToWire() ?? "running";
        var duration = a.FinishedAt is { } f ? $" · {Duration(f - a.StartedAt)}" : "";
        return $"<b>Attempt {a.Attempt}</b> <span class=\"outcome {outcome}\">{outcome}</span><span class=\"muted\">{duration}</span>";
    }

    /// Top-level object properties as a key/value list; anything else as formatted JSON.
    private static string ArgsCard(JsonElement args)
    {
        if (args.ValueKind != JsonValueKind.Object)
            return $"<div class=\"card pad\"><pre>{E(JsonSerializer.Serialize(args, PrettyJson))}</pre></div>";
        var sb = new StringBuilder("<div class=\"card\"><dl class=\"kv\">");
        var any = false;
        foreach (var p in args.EnumerateObject())
        {
            any = true;
            var v = p.Value.ValueKind switch
            {
                JsonValueKind.String => E(p.Value.GetString()!),
                JsonValueKind.Object or JsonValueKind.Array => $"<pre>{E(JsonSerializer.Serialize(p.Value, PrettyJson))}</pre>",
                _ => E(p.Value.GetRawText()),
            };
            sb.Append("<dt class=\"mono\">").Append(E(p.Name)).Append("</dt><dd class=\"mono\">").Append(v).Append("</dd>");
        }
        if (!any) sb.Append("<dd class=\"muted\">No arguments.</dd>");
        return sb.Append("</dl></div>").ToString();
    }

    private static string Meta(string label, string value) => $"<div><dt>{label}</dt><dd>{value}</dd></div>";

    private static string Humanize(string reason) => reason.Length == 0 ? reason : char.ToUpperInvariant(reason[0]) + reason[1..].Replace('_', ' ');

    private static string Duration(TimeSpan d) =>
        d.TotalMilliseconds < 1000 ? $"{d.TotalMilliseconds:0} ms" : d.TotalSeconds < 60 ? $"{d.TotalSeconds:0.#} s" : $"{d.TotalMinutes:0.#} min";

    /// Relative time for scanning, exact time in the viewer's zone on hover. Computed server-side: the page runs no script.
    private static string Ago(DateTime t, DateTime now, TimeZoneInfo tz) =>
        $"<time datetime=\"{t:O}\" title=\"{Exact(t, tz)}\">{Relative(t, now)}</time>";

    /// Absolute time in the viewer's zone; the relative age on hover.
    private static string Local(DateTime t, TimeZoneInfo tz, DateTime now)
    {
        var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(t, DateTimeKind.Utc), tz);
        var today = TimeZoneInfo.ConvertTimeFromUtc(now, tz).Date;
        var text = local.Date == today ? $"Today {local:HH:mm:ss}"
            : local.Year == today.Year ? local.ToString("MMM d, HH:mm:ss", CultureInfo.InvariantCulture)
            : local.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        return $"<time datetime=\"{t:O}\" title=\"{Exact(t, tz)} · {Relative(t, now)}\">{text}</time>";
    }

    private static string Exact(DateTime t, TimeZoneInfo tz) =>
        $"{TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(t, DateTimeKind.Utc), tz):yyyy-MM-dd HH:mm:ss.fff} {E(tz.Id)}";

    private static string Relative(DateTime t, DateTime now)
    {
        var d = now - t;
        var future = d < TimeSpan.Zero;
        var a = future ? -d : d;
        var text = a.TotalSeconds < 5 ? "just now"
            : a.TotalSeconds < 60 ? $"{a.TotalSeconds:0} s"
            : a.TotalMinutes < 60 ? $"{a.TotalMinutes:0} min"
            : a.TotalHours < 24 ? $"{a.TotalHours:0} h"
            : $"{a.TotalDays:0} d";
        return text == "just now" ? text : future ? $"in {text}" : $"{text} ago";
    }

    private const string OptionsKey = "BackgroundJobs.Dashboard";
    private const string ThemeCookie = "bj_theme";
    private const string ZoneCookie = "bj_tz";

    private static DashboardOptions OptionsOf(HttpContext http) => http.Items[OptionsKey] as DashboardOptions ?? new DashboardOptions();

    private static TimeZoneInfo Zone(HttpContext http) =>
        http.Request.Cookies[ZoneCookie] is { } id && TimeZoneInfo.TryFindSystemTimeZoneById(id, out var tz) ? tz : OptionsOf(http).TimeZone;

    private static string Theme(HttpContext http) => http.Request.Cookies[ThemeCookie] switch { "light" => "light", "dark" => "dark", _ => "system" };

    /// Header controls: a one-click theme toggle and a time zone picker, both plain form posts.
    private static string Preferences(HttpContext http, string basePath)
    {
        var action = $"{basePath}/preferences";
        var back = E(http.Request.Path + http.Request.QueryString);
        var theme = Theme(http);
        // From "system" the toggle goes to the opposite of light: we cannot know the OS theme server-side, dark is the likely ask.
        var nextTheme = theme == "dark" ? "light" : "dark";
        var zone = Zone(http);
        var now = DateTime.UtcNow;
        var sb = new StringBuilder("<div class=\"prefs\">");
        sb.Append("<details class=\"tzpick\"><summary class=\"btn ghost\" title=\"Time zone\">").Append(E(zone.Id)).Append("</summary>")
          .Append("<form method=\"post\" action=\"").Append(action).Append("\" class=\"card pad pop\"><input type=\"hidden\" name=\"return\" value=\"").Append(back).Append("\">")
          .Append("<label for=\"tz\">Time zone</label><select id=\"tz\" name=\"tz\">");
        foreach (var z in TimeZoneInfo.GetSystemTimeZones().OrderBy(z => z.GetUtcOffset(now)).ThenBy(z => z.Id, StringComparer.Ordinal))
        {
            var off = z.GetUtcOffset(now);
            sb.Append("<option value=\"").Append(E(z.Id)).Append('"').Append(z.Id == zone.Id ? " selected" : "").Append(">(UTC")
              .Append(off < TimeSpan.Zero ? "-" : "+").Append(off.ToString(@"hh\:mm")).Append(") ").Append(E(z.Id)).Append("</option>");
        }
        sb.Append("</select><button class=\"btn\" type=\"submit\">Save</button></form></details>");
        sb.Append("<form method=\"post\" action=\"").Append(action).Append("\"><input type=\"hidden\" name=\"return\" value=\"").Append(back)
          .Append("\"><button class=\"btn ghost icon\" type=\"submit\" name=\"theme\" value=\"").Append(nextTheme).Append("\" title=\"Switch to ")
          .Append(nextTheme).Append(" theme\" aria-label=\"Switch to ").Append(nextTheme).Append(" theme\">")
          .Append(nextTheme == "dark" ? MoonIcon : SunIcon).Append("</button></form></div>");
        return sb.ToString();
    }

    private const string MoonIcon = "<svg width=\"16\" height=\"16\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z\"/></svg>";
    private const string SunIcon = "<svg width=\"16\" height=\"16\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"12\" cy=\"12\" r=\"4\"/><path d=\"M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4\"/></svg>";

    private static IResult NotFound(HttpContext http, string basePath, string id) =>
        Page(http, 404, "Not found", $"<a class=\"back\" href=\"{basePath}\">&larr; All jobs</a>" + Alert($"Job {id} not found."), basePath);

    private static IResult Page(HttpContext http, int status, string title, string body, string basePath, bool autoRefresh = false)
    {
        var headers = http.Response.Headers;
        headers.ContentSecurityPolicy = "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";
        headers.XContentTypeOptions = "nosniff";
        headers["Referrer-Policy"] = "no-referrer";
        headers.CacheControl = "no-store";

        var html = $$"""
            <!doctype html>
            <html lang="en"{{(Theme(http) is "system" ? "" : $" data-theme=\"{Theme(http)}\"")}}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
            {{(autoRefresh ? $"<meta http-equiv=\"refresh\" content=\"{RefreshSeconds}\">" : "")}}
            <title>{{E(title)}} · Background Jobs</title><style>{{Css}}</style></head>
            <body><header class="top"><a href="{{basePath}}" class="brand"><span class="logo"></span>Background Jobs</a>{{Preferences(http, basePath)}}</header>
            <main>{{body}}</main></body></html>
            """;
        return Results.Content(html, "text/html; charset=utf-8", Encoding.UTF8, status);
    }

    private static string ListUrl(string basePath, JobState? state, Guid? after, bool autoRefresh)
    {
        var q = new List<string>();
        if (state != null) q.Add($"state={state.Value.ToWire()}");
        if (after != null) q.Add($"after={after}");
        if (!autoRefresh) q.Add("refresh=off");
        return q.Count == 0 ? basePath : $"{basePath}?{string.Join("&amp;", q)}";
    }

    private static string Tab(string basePath, JobState? state, JobState? current, bool autoRefresh) =>
        $"<a class=\"tab{(state == current ? " active" : "")}\" href=\"{ListUrl(basePath, state, null, autoRefresh)}\">{state?.ToWire() ?? "all"}</a>";

    private static string RefreshToggle(string basePath, JobState? state, Guid? after, bool on) =>
        $"<a class=\"btn outline\" href=\"{ListUrl(basePath, state, after, !on)}\"><span class=\"pulse{(on ? " on" : "")}\"></span>Auto-refresh {(on ? "on" : "off")}</a>";

    private static string ActionForm(string action, string label, string cls) =>
        $"<form method=\"post\" action=\"{action}\"><button class=\"{cls}\" type=\"submit\">{label}</button></form>";

    private static string Badge(JobState s) => $"<span class=\"badge {s.ToWire()}\">{s.ToWire()}</span>";
    private static string Alert(string text, bool info = false) => $"<div class=\"alert{(info ? " info" : "")}\">{E(text)}</div>";
    private static string E(string s) => WebUtility.HtmlEncode(s);

    // Tokens and component shapes follow shadcn/ui (zinc theme) so the page needs no Tailwind build or CDN.
    private const string Css = """
        :root{--background:#fff;--foreground:#09090b;--muted:#f4f4f5;--muted-foreground:#71717a;--border:#e4e4e7;--card:#fff;
        --primary:#18181b;--primary-foreground:#fafafa;--destructive:#dc2626;--radius:.5rem;color-scheme:light dark}
        @media (prefers-color-scheme:dark){:root:not([data-theme=light]){--background:#09090b;--foreground:#fafafa;--muted:#27272a;--muted-foreground:#a1a1aa;
        --border:#27272a;--card:#0c0c0e;--primary:#fafafa;--primary-foreground:#18181b;--destructive:#ef4444;color-scheme:dark}}
        :root[data-theme=dark]{--background:#09090b;--foreground:#fafafa;--muted:#27272a;--muted-foreground:#a1a1aa;
        --border:#27272a;--card:#0c0c0e;--primary:#fafafa;--primary-foreground:#18181b;--destructive:#ef4444;color-scheme:dark}
        :root[data-theme=light]{color-scheme:light}
        *{box-sizing:border-box}body{margin:0;background:var(--background);color:var(--foreground);
        font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
        a{color:inherit}main{max-width:1100px;margin:0 auto;padding:24px 16px 64px}
        .top{border-bottom:1px solid var(--border);padding:12px 16px}.brand{font-weight:600;text-decoration:none;display:inline-flex;gap:8px;align-items:center}
        .logo{width:18px;height:18px;border-radius:5px;background:var(--primary)}
        h1{font-size:24px;letter-spacing:-.02em;margin:0;display:flex;gap:10px;align-items:center;flex-wrap:wrap}h2{font-size:15px;margin:0 0 12px}
        p{margin:4px 0 0}.muted{color:var(--muted-foreground)}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
        .head{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:20px}
        .back{display:inline-block;margin-bottom:12px;color:var(--muted-foreground);text-decoration:none}.back:hover{color:var(--foreground)}
        .card{background:var(--card);border:1px solid var(--border);border-radius:calc(var(--radius) + 4px);box-shadow:0 1px 2px rgb(0 0 0/.05);overflow:auto}
        .pad{padding:20px}.stat{display:flex;flex-direction:column;gap:2px}.stat b{font-weight:600}
        .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:16px}
        .cols{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-bottom:16px}
        table{width:100%;border-collapse:collapse}th{text-align:left;font-weight:500;color:var(--muted-foreground);height:40px;padding:0 16px}
        td{padding:10px 16px;border-top:1px solid var(--border)}tbody tr:hover{background:var(--muted)}td a{font-weight:500;text-decoration:none}td a:hover{text-decoration:underline}
        .num{text-align:right}.empty{text-align:center;color:var(--muted-foreground);padding:40px}
        .tabs{display:inline-flex;flex-wrap:wrap;gap:2px;background:var(--muted);padding:4px;border-radius:var(--radius);margin-bottom:16px}
        .tab{padding:4px 12px;border-radius:calc(var(--radius) - 2px);text-decoration:none;color:var(--muted-foreground);font-weight:500}
        .tab.active{background:var(--background);color:var(--foreground);box-shadow:0 1px 2px rgb(0 0 0/.1)}
        .btn{display:inline-flex;align-items:center;gap:8px;height:36px;padding:0 16px;border-radius:var(--radius);border:1px solid transparent;
        background:var(--primary);color:var(--primary-foreground);font:inherit;font-weight:500;cursor:pointer;text-decoration:none}
        .btn:hover{opacity:.9}.btn.outline{background:var(--background);color:var(--foreground);border-color:var(--border)}.btn.outline:hover{background:var(--muted)}
        .btn.destructive{background:var(--destructive);color:#fff}.actions{display:flex;gap:8px}form{margin:0}
        .pager{display:flex;gap:8px;justify-content:flex-end;margin-top:16px}
        .badge{display:inline-flex;border-radius:999px;padding:1px 10px;font-size:12px;font-weight:600;border:1px solid var(--border);vertical-align:middle;
        background:var(--muted);color:var(--muted-foreground)}
        .badge.enqueued{background:#dbeafe;color:#1d4ed8;border-color:#bfdbfe}.badge.processing,.badge.running{background:#fef3c7;color:#b45309;border-color:#fde68a}
        .badge.succeeded{background:#dcfce7;color:#15803d;border-color:#bbf7d0}.badge.failed{background:#fee2e2;color:#b91c1c;border-color:#fecaca}
        .badge.interrupted{background:#ffedd5;color:#c2410c;border-color:#fed7aa}.badge.scheduled{background:#ede9fe;color:#6d28d9;border-color:#ddd6fe}
        @media (prefers-color-scheme:dark){:root:not([data-theme=light]) .badge.enqueued{background:rgb(59 130 246/.15);color:#93c5fd;border-color:rgb(59 130 246/.3)}:root:not([data-theme=light]) .badge.processing,:root:not([data-theme=light]) .badge.running{background:rgb(245 158 11/.15);color:#fcd34d;border-color:rgb(245 158 11/.3)}
        :root:not([data-theme=light]) .badge.succeeded{background:rgb(34 197 94/.15);color:#86efac;border-color:rgb(34 197 94/.3)}:root:not([data-theme=light]) .badge.failed{background:rgb(239 68 68/.15);color:#fca5a5;border-color:rgb(239 68 68/.3)}
        :root:not([data-theme=light]) .badge.interrupted{background:rgb(249 115 22/.15);color:#fdba74;border-color:rgb(249 115 22/.3)}:root:not([data-theme=light]) .badge.scheduled{background:rgb(139 92 246/.15);color:#c4b5fd;border-color:rgb(139 92 246/.3)}}
        :root[data-theme=dark] .badge.enqueued{background:rgb(59 130 246/.15);color:#93c5fd;border-color:rgb(59 130 246/.3)}:root[data-theme=dark] .badge.processing,:root[data-theme=dark] .badge.running{background:rgb(245 158 11/.15);color:#fcd34d;border-color:rgb(245 158 11/.3)}
        :root[data-theme=dark] .badge.succeeded{background:rgb(34 197 94/.15);color:#86efac;border-color:rgb(34 197 94/.3)}:root[data-theme=dark] .badge.failed{background:rgb(239 68 68/.15);color:#fca5a5;border-color:rgb(239 68 68/.3)}
        :root[data-theme=dark] .badge.interrupted{background:rgb(249 115 22/.15);color:#fdba74;border-color:rgb(249 115 22/.3)}:root[data-theme=dark] .badge.scheduled{background:rgb(139 92 246/.15);color:#c4b5fd;border-color:rgb(139 92 246/.3)}
        pre{margin:8px 0 0;padding:12px;background:var(--muted);border-radius:var(--radius);overflow:auto;font-size:12px;white-space:pre-wrap;word-break:break-word}
        pre.error{color:var(--destructive)}
        .alert{border:1px solid var(--destructive);color:var(--destructive);border-radius:var(--radius);padding:12px 16px;margin-bottom:16px}
        .alert.info{border-color:var(--border);color:var(--foreground);background:var(--muted)}
        .pulse{width:8px;height:8px;border-radius:50%;background:var(--muted-foreground)}.pulse.on{background:#22c55e;box-shadow:0 0 0 3px rgb(34 197 94/.25)}
        .callout{border:1px solid #fecaca;background:#fef2f2;color:#7f1d1d;border-radius:calc(var(--radius) + 4px);padding:14px 16px;margin-bottom:16px}
        .callout pre{background:rgb(255 255 255/.6);color:#b91c1c}
        @media (prefers-color-scheme:dark){:root:not([data-theme=light]) .callout{background:#2a1215;border-color:#7f1d1d;color:#fecaca}
        :root:not([data-theme=light]) .callout pre{background:rgb(0 0 0/.3);color:#fca5a5}}
        :root[data-theme=dark] .callout{background:#2a1215;border-color:#7f1d1d;color:#fecaca}:root[data-theme=dark] .callout pre{background:rgb(0 0 0/.3);color:#fca5a5}
        .meta{margin-bottom:20px}.meta dl{display:flex;flex-wrap:wrap;margin:0}.meta dl>div{flex:1 1 120px;padding:12px 16px;border-left:1px solid var(--border)}
        .meta dl>div:first-child{border-left:0}dt{color:var(--muted-foreground);font-size:12px}dd{margin:2px 0 0;font-weight:500}
        .split{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:20px;align-items:start}@media (max-width:820px){.split{grid-template-columns:1fr}}
        .run{margin-bottom:12px;overflow:visible}.run summary{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:12px 16px;cursor:pointer;list-style-position:inside}
        .run[open] summary{border-bottom:1px solid var(--border)}
        .ev{flex:1;min-width:0}.line{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}.when{margin-left:auto;font-size:12px}
        .state{font-size:12px;color:var(--muted-foreground)}
        .log{table-layout:fixed}.log td{padding:9px 16px;vertical-align:top}.log tr:first-child td{border-top:0}.log tbody tr:hover{background:none}
        .log .t{width:120px;padding-top:11px}.log .s{width:110px;text-align:right;font-size:12px;padding-top:11px}
        .log pre{margin:6px 0 0;padding:8px 10px;font-size:12px}.outcome{font-size:12px;margin-left:6px;color:var(--muted-foreground)}
        .outcome.failed{color:var(--destructive)}.outcome.interrupted{color:#c2410c}
        .top{display:flex;justify-content:space-between;align-items:center;gap:12px}.prefs{display:flex;gap:4px;align-items:center}
        .btn.ghost{background:transparent;color:var(--muted-foreground);border-color:transparent;height:32px;padding:0 10px;font-size:13px}
        .btn.ghost:hover{background:var(--muted);color:var(--foreground)}.btn.icon{width:32px;padding:0;justify-content:center}
        .tzpick{position:relative}.tzpick summary{list-style:none}.tzpick summary::-webkit-details-marker{display:none}
        .pop{position:absolute;right:0;top:38px;z-index:10;width:320px;display:flex;flex-direction:column;gap:8px;overflow:visible}
        .nowrap{white-space:nowrap}.id{font-size:12px}
        .kv{margin:0;display:grid;grid-template-columns:auto 1fr}.kv dt,.kv dd{padding:10px 16px;border-top:1px solid var(--border);margin:0}
        .kv dt:first-of-type,.kv dt:first-of-type+dd{border-top:0}.kv dd{word-break:break-word;font-weight:400}.kv pre{margin:0}
        .picker{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}
        .pick{display:flex;flex-direction:column;gap:4px;text-decoration:none}.pick:hover{border-color:var(--foreground)}
        .form{max-width:640px;display:flex;flex-direction:column;gap:16px}fieldset{border:0;margin:0;padding:0;display:flex;flex-direction:column;gap:14px}
        legend{font-weight:600;margin-bottom:10px}.field{display:flex;flex-direction:column;gap:6px}.field label{font-weight:500}
        input:not([type=checkbox]),select,textarea{height:36px;padding:0 12px;border:1px solid var(--border);border-radius:var(--radius);
        background:var(--background);color:var(--foreground);font:inherit}textarea{height:auto;padding:8px 12px}
        input:focus,select:focus,textarea:focus{outline:2px solid var(--foreground);outline-offset:1px}
        .check{display:flex;gap:8px;align-items:center}.hint{color:var(--muted-foreground);margin:0;font-size:11px}
        .opts summary{cursor:pointer;font-weight:600}.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-top:12px}
        .actions.end{justify-content:flex-end}.alert ul{margin:6px 0 0;padding-left:18px}
        """;
}
