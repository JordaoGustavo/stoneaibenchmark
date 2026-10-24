using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Primitives;

namespace BackgroundJobs;

/// <summary>
/// Continuations from the dashboard: the "Run after" part of the trigger form and the chain builder (/chain).
/// Like the rest of the dashboard it runs no script: the chain lives in the form, and every edit (add, move, remove a
/// step) is a post that re-renders it.
/// </summary>
public static partial class DashboardEndpoints
{
    private const int MaxChainSteps = 20;
    private const string OnSucceeded = "on_succeeded";
    private const string OnFinished = "on_finished";

    private sealed record ChainStep(string Job, string Condition, Dictionary<string, StringValues>? Values);

    private static void MapChainBuilder(RouteGroupBuilder group, string basePath)
    {
        group.MapGet("/chain", async (HttpContext http, JobRegistry registry, IJobClient client, string? after, CancellationToken ct) =>
            ChainPage(http, basePath, registry, [], after ?? "", OnSucceeded, await FindParentAsync(client, after, ct), [], new()));

        group.MapPost("/chain", async (HttpContext http, JobRegistry registry, IJobClient client, CancellationToken ct) =>
        {
            if (IsCrossSite(http)) return Page(http, 403, "Forbidden", Alert("Cross-site request rejected."), basePath);
            var form = await http.Request.ReadFormAsync(ct);
            var steps = ReadSteps(form);
            // "Add step" on the single-job form: that job, with everything typed, becomes step 1.
            if (form["single"].LastOrDefault() is { Length: > 0 } single && steps.Count == 0)
                steps.Add(new ChainStep(single, OnSucceeded, form.Where(kv => kv.Key.StartsWith("arg.", StringComparison.Ordinal) || kv.Key.StartsWith("opt.", StringComparison.Ordinal))
                    .ToDictionary(kv => kv.Key, kv => kv.Value)));
            var afterValue = form["after.id"].LastOrDefault() ?? "";
            var parent = await FindParentAsync(client, afterValue, ct);
            var op = form["op"].LastOrDefault() ?? "";
            var errors = new List<string>();

            // Editing ops only reshape the chain; nothing is validated or written until "enqueue".
            if (op == "add")
            {
                var job = form["add.job"].LastOrDefault() ?? "";
                if (!registry.Registrations.Any(r => r.Name == job)) errors.Add("Pick a job type to add.");
                else if (steps.Count >= MaxChainSteps) errors.Add($"A chain has at most {MaxChainSteps} steps.");
                else steps.Add(new ChainStep(job, OnSucceeded, null));
            }
            else if (Indexed(op, "remove:", steps.Count) is { } r) steps.RemoveAt(r);
            else if (Indexed(op, "up:", steps.Count) is { } u && u > 0) (steps[u - 1], steps[u]) = (steps[u], steps[u - 1]);
            else if (Indexed(op, "down:", steps.Count) is { } d && d < steps.Count - 1) (steps[d + 1], steps[d]) = (steps[d], steps[d + 1]);
            else if (op == "enqueue") return await EnqueueChainAsync(http, basePath, registry, client, form, steps, afterValue, parent, ct);
            return ChainPage(http, basePath, registry, steps, afterValue, form["after.condition"].LastOrDefault() ?? OnSucceeded, parent, errors, new(), errors.Count > 0 ? 400 : 200);
        });

        group.MapGet("/chain/result", async (HttpContext http, IJobClient client, TimeProvider time, string? ids, CancellationToken ct) =>
        {
            var jobs = new List<Job>();
            foreach (var part in (ids ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries).Take(MaxChainSteps))
            {
                if (!Guid.TryParse(part, out var id)) continue;
                try { jobs.Add(await client.GetAsync(id, ct)); }
                catch (JobNotFoundException) { }
            }
            if (jobs.Count == 0) return Page(http, 404, "Not found", $"<a class=\"back\" href=\"{basePath}/chain\">&larr; Chain builder</a>" + Alert("No jobs of this chain were found."), basePath);
            return ChainResult(http, basePath, jobs, time.GetUtcNow().UtcDateTime);
        });
    }

    /// <summary>
    /// Validates every step (and the optional parent) first, so an invalid step enqueues nothing. Steps are then enqueued
    /// one by one, each continuing after the previous; IJobClient has no batch insert whose jobs can reference each other.
    /// If an enqueue fails midway (e.g. the store is down), the steps already created are cancelled, best effort.
    /// </summary>
    private static async Task<IResult> EnqueueChainAsync(HttpContext http, string basePath, JobRegistry registry, IJobClient client,
        IFormCollection form, List<ChainStep> steps, string afterValue, Job? parent, CancellationToken ct)
    {
        var errors = new List<string>();
        var invalid = new Dictionary<int, List<string>>();
        var tz = Zone(http);
        var (parentId, firstCondition) = await ParseRunAfterAsync(client, form, "after.", errors, ct);
        if (steps.Count == 0) errors.Add("Add at least one step.");

        var prepared = new List<(string Name, object Args, JobOptions? Options, ContinuationCondition Condition)>();
        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            var stepErrors = new List<string>();
            var reg = registry.Registrations.FirstOrDefault(r => r.Name == step.Job);
            if (reg.Name is null) stepErrors.Add($"job type \"{step.Job}\" is not registered.");
            else
            {
                var args = DashboardForms.Parse(reg.ArgsType, form, tz, stepErrors, scope: $"s{i}.");
                var options = ParseOptions(http, form, tz, stepErrors, $"s{i}.opt.");
                var condition = i == 0 ? firstCondition : ParseCondition(step.Condition, stepErrors);
                prepared.Add((step.Job, args, options, condition));
            }
            if (stepErrors.Count == 0) continue;
            invalid[i] = stepErrors;
            errors.AddRange(stepErrors.Select(e => $"Step {i + 1} ({step.Job}): {e}"));
        }
        if (errors.Count > 0) return ChainPage(http, basePath, registry, steps, afterValue, form["after.condition"].LastOrDefault() ?? OnSucceeded, parent, errors, invalid, 400);

        var created = new List<Guid>();
        Guid? previous = parentId;
        try
        {
            foreach (var (name, args, stepOptions, condition) in prepared)
            {
                var options = previous is null ? stepOptions : (stepOptions ?? new JobOptions()) with { ContinueAfter = previous, Condition = condition };
                previous = await client.EnqueueAsync(name, args, options, ct);
                created.Add(previous.Value);
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            foreach (var id in Enumerable.Reverse(created))
            {
                try { await client.CancelAsync(id, CancellationToken.None); }
                catch (Exception) { /* best effort: report below */ }
            }
            var message = e is JobNotFoundException ? $"Run after: job {parentId} no longer exists." : e.Message;
            var undone = created.Count == 0 ? "Nothing was enqueued."
                : $"The {created.Count} step(s) already enqueued were cancelled: {string.Join(", ", created)}.";
            return ChainPage(http, basePath, registry, steps, afterValue, form["after.condition"].LastOrDefault() ?? OnSucceeded, parent, [$"Step {created.Count + 1} could not be enqueued: {message}", undone], new(), 400);
        }
        var query = string.Join(',', created);
        return Results.Redirect($"{basePath}/chain/result?ids={query}");
    }

    private static List<ChainStep> ReadSteps(IFormCollection form)
    {
        var count = int.TryParse(form["steps"].LastOrDefault(), out var n) ? Math.Clamp(n, 0, MaxChainSteps) : 0;
        var steps = new List<ChainStep>();
        for (var i = 0; i < count; i++)
        {
            var scope = $"s{i}.";
            var job = form[scope + "job"].LastOrDefault();
            if (string.IsNullOrEmpty(job)) continue;
            // Kept without the step prefix, so a step re-renders under its new index after a move.
            var values = form.Where(kv => kv.Key.StartsWith(scope + "arg.", StringComparison.Ordinal) || kv.Key.StartsWith(scope + "opt.", StringComparison.Ordinal))
                .ToDictionary(kv => kv.Key[scope.Length..], kv => kv.Value);
            steps.Add(new ChainStep(job, form[scope + "cond"].LastOrDefault() ?? OnSucceeded, values));
        }
        return steps;
    }

    private static int? Indexed(string op, string prefix, int count) =>
        op.StartsWith(prefix, StringComparison.Ordinal) && int.TryParse(op[prefix.Length..], out var i) && i >= 0 && i < count ? i : null;

    private static IResult ChainPage(HttpContext http, string basePath, JobRegistry registry, List<ChainStep> steps, string afterValue, string afterCondition, Job? parent,
        List<string> errors, Dictionary<int, List<string>> invalid, int status = 200)
    {
        var tz = Zone(http);
        var body = new StringBuilder();
        body.Append("<a class=\"back\" href=\"").Append(basePath).Append("/new\">&larr; All job types</a>")
            .Append("<div class=\"head\"><div><h1>Build a chain</h1><p class=\"muted\">Jobs that run one after another. Each step waits in <b>awaiting</b> until the one before it finishes, then runs or is cancelled by its condition.</p></div></div>");
        if (errors.Count > 0)
            body.Append("<div class=\"alert\"><b>Could not enqueue the chain</b><ul>").Append(string.Concat(errors.Select(e => $"<li>{E(e)}</li>"))).Append("</ul></div>");

        body.Append("<form class=\"chainform\" method=\"post\" action=\"").Append(basePath).Append("/chain\">")
            // Enter in a text field submits the first button: make that a harmless re-render, not "enqueue".
            .Append("<button class=\"sr-only\" type=\"submit\" name=\"op\" value=\"refresh\" formnovalidate tabindex=\"-1\" aria-hidden=\"true\"></button>")
            .Append("<input type=\"hidden\" name=\"steps\" value=\"").Append(steps.Count).Append("\">")
            .Append("<ol class=\"chain\">");

        // Optional anchor: an existing job the whole chain continues after.
        body.Append("<li class=\"step\"><span class=\"dot parent\">").Append(ChainIcon).Append("</span><div class=\"card pad\">")
            .Append(RunAfterSection(basePath, "after.", afterValue, parent, afterCondition, chain: true))
            .Append("</div></li>");

        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            var scope = $"s{i}.";
            var reg = registry.Registrations.FirstOrDefault(r => r.Name == step.Job);
            body.Append(i > 0 ? Connector(scope + "cond", step.Condition, $"step {i}") : Rail);
            body.Append("<li class=\"step").Append(invalid.ContainsKey(i) ? " invalid" : "").Append("\"><span class=\"dot\">").Append(i + 1).Append("</span>")
                .Append("<div class=\"card\"><div class=\"stephead\"><div><b>").Append(E(step.Job)).Append("</b> <span class=\"mono muted\">")
                .Append(E(reg.ArgsType?.Name ?? "unregistered")).Append("</span></div><div class=\"actions\">")
                .Append(StepButton($"up:{i}", "Move up", "&uarr;", i == 0))
                .Append(StepButton($"down:{i}", "Move down", "&darr;", i == steps.Count - 1))
                .Append(StepButton($"remove:{i}", "Remove step", "&times;", false))
                .Append("</div></div><div class=\"stepbody\"><input type=\"hidden\" name=\"").Append(scope).Append("job\" value=\"").Append(E(step.Job)).Append("\">");
            if (invalid.TryGetValue(i, out var stepErrors))
                body.Append("<ul class=\"steperr\">").Append(string.Concat(stepErrors.Select(e => $"<li>{E(e)}</li>"))).Append("</ul>");
            if (reg.ArgsType != null)
            {
                IFormCollection? values = step.Values is null ? null
                    : new FormCollection(step.Values.ToDictionary(kv => scope + kv.Key, kv => kv.Value));
                body.Append("<div class=\"args\">").Append(DashboardForms.Render(reg.ArgsType, values, tz, scope)).Append("</div>").Append(OptionsFields(http, step.Job, values, tz, scope + "opt.", $"o{i}-"));
            }
            body.Append("</div></div></li>");
        }

        body.Append(Rail);
        body.Append("<li class=\"step\"><span class=\"dot ghost\">+</span>").Append(AddStepRow(basePath, registry, steps.Count, null)).Append("</li></ol>");

        body.Append("<div class=\"chainfoot\"><p class=\"muted\">").Append(steps.Count == 0 ? "No steps yet." : $"{steps.Count} step{(steps.Count == 1 ? "" : "s")}.")
            .Append(" Every step is validated before anything is enqueued.</p><div class=\"actions\"><a class=\"btn outline\" href=\"").Append(basePath).Append("/new\">Cancel</a>")
            .Append("<button class=\"btn\" type=\"submit\" name=\"op\" value=\"enqueue\"").Append(steps.Count == 0 ? " disabled" : "").Append(">Enqueue chain</button></div></div></form>");
        return Page(http, status, "Build a chain", body.ToString(), basePath);
    }

    /// <summary>
    /// The dashed "Next step" row. On the single-job form (<paramref name="single"/>) it posts the whole form to the chain
    /// builder, where that job becomes step 1.
    /// </summary>
    private static string AddStepRow(string basePath, JobRegistry registry, int steps, string? single)
    {
        var sb = new StringBuilder("<div class=\"addstep\">");
        if (single != null) sb.Append("<input type=\"hidden\" name=\"single\" value=\"").Append(E(single)).Append("\">");
        sb.Append("<label for=\"add-job\"><b>").Append(steps == 0 ? "First step" : "Next step").Append("</b>")
          .Append(single != null ? " <span class=\"muted\">run another job after this one</span>" : "").Append("</label><select id=\"add-job\" name=\"add.job\">");
        foreach (var (name, _) in registry.Registrations) sb.Append("<option value=\"").Append(E(name)).Append("\">").Append(E(name)).Append("</option>");
        sb.Append("</select><button class=\"btn outline\" type=\"submit\" name=\"op\" value=\"add\" formnovalidate");
        if (single != null) sb.Append(" formaction=\"").Append(basePath).Append("/chain\"");
        return sb.Append(steps >= MaxChainSteps || registry.Registrations.Count == 0 ? " disabled" : "").Append(">Add step</button></div>").ToString();
    }

    private const string Rail = "<li class=\"link\" aria-hidden=\"true\"><span class=\"rail\"></span><span></span></li>";

    private static string StepButton(string op, string label, string glyph, bool disabled) =>
        $"<button class=\"btn ghost icon\" type=\"submit\" name=\"op\" value=\"{op}\" formnovalidate title=\"{label}\" aria-label=\"{label}\"{(disabled ? " disabled" : "")}>{glyph}</button>";

    /// <summary>
    /// The rail between two steps, carrying the condition of the step below it as a segmented control.
    /// </summary>
    private static string Connector(string name, string condition, string previous) =>
        $"<li class=\"link\"><span class=\"rail\"></span><div class=\"seg\" role=\"radiogroup\" aria-label=\"When to run\">" +
        Segment(name, OnSucceeded, condition, "If it succeeds", $"Runs only if {previous} succeeds; otherwise it is cancelled.") +
        Segment(name, OnFinished, condition, "When it finishes", $"Runs once {previous} ends: succeeded, failed or cancelled.") + "</div></li>";

    private static string Segment(string name, string value, string current, string label, string title) =>
        $"<label title=\"{E(title)}\"><input type=\"radio\" name=\"{name}\" value=\"{value}\"{(current == value ? " checked" : "")}><span>{label}</span></label>";

    private static string Choice(string name, string value, string current, string label, string text) =>
        $"<label class=\"choice\"><input type=\"radio\" name=\"{name}\" value=\"{value}\"{(current == value ? " checked" : "")}><span><b>{label}</b><span class=\"muted\">{text}</span></span></label>";

    /// <summary>
    /// "Run after": a parent job id and the condition. Shows the resolved parent and what enqueueing will do with it.
    /// </summary>
    private static string RunAfterSection(string basePath, string scope, string value, Job? parent, string? condition, bool chain = false)
    {
        var current = condition == OnFinished ? OnFinished : OnSucceeded;
        var sb = new StringBuilder("<fieldset class=\"runafter\"><legend>").Append(chain ? "Start after" : "Run after")
            .Append(" <span class=\"muted\">(optional)</span></legend><p class=\"hint\">")
            .Append(chain ? "Leave blank to start the chain now, or continue an existing job." : "Leave blank to run on its own, or chain this job to an existing one.")
            .Append("</p><div class=\"field\"><label for=\"").Append(scope).Append("id\">Parent job id</label><input class=\"mono\" type=\"text\" id=\"")
            .Append(scope).Append("id\" name=\"").Append(scope).Append("id\" value=\"").Append(E(value))
            .Append("\" placeholder=\"00000000-0000-0000-0000-000000000000\" autocomplete=\"off\" spellcheck=\"false\"></div>");
        if (parent != null) sb.Append(ParentBanner(basePath, parent));
        sb.Append("<div class=\"choices\" role=\"radiogroup\" aria-label=\"Condition\">")
            .Append(Choice(scope + "condition", OnSucceeded, current, "On success", "Runs only if the parent succeeds; if it fails or is cancelled, this job is cancelled."))
            .Append(Choice(scope + "condition", OnFinished, current, "When finished", "Runs once the parent ends in any state: succeeded, failed or cancelled."))
            .Append("</div></fieldset>");
        return sb.ToString();
    }

    /// <summary>
    /// The parent at a glance and what will happen: wait in awaiting, or (finished parent) be decided at once.
    /// </summary>
    private static string ParentBanner(string basePath, Job parent)
    {
        var finished = JobStateWire.Finished.Contains(parent.State);
        return $"<div class=\"parentchip{(finished ? " done" : "")}\"><div class=\"line\"><span class=\"muted\">After</span> <a href=\"{basePath}/{parent.Id}\"><b>{E(parent.Name)}</b></a> " +
               $"{Badge(parent.State)} <span class=\"mono muted\">{parent.Id}</span></div><p class=\"muted\">" +
               (finished
                   ? "This job has already finished, so the condition is decided as soon as you enqueue: the new job runs right away, or is cancelled if the condition is not met."
                   : "The new job waits in <b>awaiting</b> until this job finishes, then runs or is cancelled by the condition.") +
               "</p></div>";
    }

    private static async Task<Job?> FindParentAsync(IJobClient client, string? value, CancellationToken ct)
    {
        if (!Guid.TryParse(value?.Trim(), out var id)) return null;
        try { return await client.GetAsync(id, ct); }
        catch (JobNotFoundException) { return null; }
    }

    /// <summary>
    /// Reads "Run after" fields under <paramref name="scope"/>. A blank id means no parent; anything else must name an existing job.
    /// </summary>
    private static async Task<(Guid? Parent, ContinuationCondition Condition)> ParseRunAfterAsync(
        IJobClient client, IFormCollection form, string scope, List<string> errors, CancellationToken ct)
    {
        var raw = form[scope + "id"].LastOrDefault()?.Trim() ?? "";
        if (raw.Length == 0) return (null, ContinuationCondition.OnSucceeded);
        var condition = ParseCondition(form[scope + "condition"].LastOrDefault() ?? OnSucceeded, errors);
        if (!Guid.TryParse(raw, out var id))
        {
            errors.Add($"Run after: \"{raw}\" is not a job id; paste the id shown on the job's page (a GUID).");
            return (null, condition);
        }
        try { await client.GetAsync(id, ct); }
        catch (JobNotFoundException)
        {
            errors.Add($"Run after: no job with id {id} exists (it may have been purged by retention).");
            return (null, condition);
        }
        return (id, condition);
    }

    private static ContinuationCondition ParseCondition(string value, List<string> errors)
    {
        switch (value)
        {
            case OnSucceeded: return ContinuationCondition.OnSucceeded;
            case OnFinished: return ContinuationCondition.OnFinished;
            default:
                errors.Add($"Condition \"{value}\" is unknown; pick on success or when finished.");
                return ContinuationCondition.OnSucceeded;
        }
    }

    private static IResult ChainResult(HttpContext http, string basePath, List<Job> jobs, DateTime now)
    {
        var tz = Zone(http);
        var body = new StringBuilder();
        body.Append("<a class=\"back\" href=\"").Append(basePath).Append("\">&larr; All jobs</a>")
            .Append("<div class=\"head\"><div><h1>Chain enqueued</h1><p class=\"muted\">").Append(jobs.Count).Append(jobs.Count == 1 ? " job" : " jobs")
            .Append(", each continuing after the one above it.</p></div><div class=\"actions\"><a class=\"btn outline\" href=\"").Append(basePath)
            .Append("/chain?after=").Append(jobs[^1].Id).Append("\">Extend chain</a><a class=\"btn\" href=\"").Append(basePath).Append("/chain\">Build another</a></div></div>");
        body.Append("<ol class=\"chain done\">");
        if (jobs[0].ContinueAfter is { } anchor)
            body.Append("<li class=\"step\"><span class=\"dot parent\">").Append(ChainIcon).Append("</span><div class=\"card pad line\"><span class=\"muted\">After</span> <a class=\"mono\" href=\"")
                .Append(basePath).Append('/').Append(anchor).Append("\">").Append(anchor).Append("</a></div></li>");
        for (var i = 0; i < jobs.Count; i++)
        {
            var j = jobs[i];
            if (i > 0 || j.ContinueAfter != null)
                body.Append("<li class=\"link").Append(j.Condition == ContinuationCondition.OnFinished ? " finished" : "").Append("\"><span class=\"rail\"></span><span class=\"muted cond\">")
                    .Append(j.Condition == ContinuationCondition.OnFinished ? "when it finishes" : "if it succeeds").Append("</span></li>");
            body.Append("<li class=\"step\"><span class=\"dot\">").Append(i + 1).Append("</span><div class=\"card pad\"><div class=\"line\"><a href=\"").Append(basePath).Append('/').Append(j.Id)
                .Append("\"><b>").Append(E(j.Name)).Append("</b></a> ").Append(Badge(j.State)).Append("<span class=\"when muted\">").Append(Local(j.CreatedAt, tz, now))
                .Append("</span></div><p class=\"mono muted\">").Append(j.Id).Append("</p></div></li>");
        }
        body.Append("</ol>");
        var running = jobs.Any(j => !JobStateWire.Finished.Contains(j.State));
        return Page(http, 200, "Chain enqueued", body.ToString(), basePath, autoRefresh: running);
    }

    private const string ChainIcon = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" aria-hidden=\"true\"><path d=\"M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7\"/><path d=\"M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7\"/></svg>";

    private const string ChainCss = """
        .sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0;padding:0}
        .btn:disabled{opacity:.4;cursor:not-allowed}.btn.ghost.icon{font-size:15px}
        .runafter{gap:12px}.runafter .hint{font-size:12px;margin-top:-6px}
        .choices{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}
        .choice{display:flex;gap:10px;align-items:flex-start;border:1px solid var(--border);border-radius:var(--radius);padding:10px 12px;cursor:pointer}
        .choice>span{display:flex;flex-direction:column;gap:2px}.choice .muted{font-size:12px}.choice input{margin-top:3px;accent-color:var(--primary)}
        .choice:hover{background:var(--muted)}.choice:has(input:checked){border-color:var(--foreground);box-shadow:0 0 0 1px var(--foreground)}
        .parentchip{border:1px solid var(--border);border-left:3px solid #0891b2;border-radius:var(--radius);padding:10px 12px;background:var(--muted)}
        .parentchip.done{border-left-color:#16a34a}.parentchip p{font-size:12px}.parentchip a{text-decoration:none}.parentchip a:hover{text-decoration:underline}
        .nochildren{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:20px;border-style:dashed}
        .chain{list-style:none;margin:0 0 8px;padding:0}
        .step{display:grid;grid-template-columns:32px minmax(0,1fr);gap:14px;align-items:start}
        .step>.card{overflow:visible}.step.invalid>.card{border-color:var(--destructive);box-shadow:0 0 0 1px var(--destructive)}
        .dot{width:32px;height:32px;border-radius:50%;display:grid;place-items:center;font-weight:600;font-size:13px;margin-top:8px;
        background:var(--primary);color:var(--primary-foreground)}
        .dot.parent{background:var(--background);color:var(--foreground);border:1px solid var(--border)}
        .dot.ghost{background:var(--background);color:var(--muted-foreground);border:1px dashed var(--muted-foreground);font-size:18px;font-weight:400}
        .link{display:grid;grid-template-columns:32px minmax(0,1fr);gap:14px;align-items:center;min-height:28px}
        .link .rail{justify-self:center;align-self:stretch;width:2px;min-height:28px;background:var(--border)}
        .link:has(input[value=on_finished]:checked) .rail,.link.finished .rail{background:repeating-linear-gradient(var(--muted-foreground) 0 4px,transparent 4px 8px)}
        .link .cond{font-size:12px;padding:6px 0}
        .seg{display:inline-flex;gap:2px;background:var(--muted);padding:3px;border-radius:var(--radius);margin:8px 0;justify-self:start}
        .seg label{cursor:pointer}.seg input{position:absolute;opacity:0;pointer-events:none}
        .seg span{display:block;padding:3px 10px;border-radius:calc(var(--radius) - 2px);font-size:12px;font-weight:500;color:var(--muted-foreground)}
        .seg label:has(input:checked) span{background:var(--background);color:var(--foreground);box-shadow:0 1px 2px rgb(0 0 0/.12)}
        .seg label:has(input:focus-visible) span{outline:2px solid var(--foreground);outline-offset:1px}
        .stephead{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:8px 8px 8px 16px;border-bottom:1px solid var(--border)}
        .stephead .actions{gap:2px}.stepbody{padding:16px;display:flex;flex-direction:column;gap:14px}.stepbody .opts{border-top:1px solid var(--border);padding-top:12px}
        .steperr{margin:0;padding:8px 12px 8px 28px;border-radius:var(--radius);color:var(--destructive);border:1px solid var(--destructive);font-size:13px}
        .addstep{display:flex;gap:10px;align-items:center;flex-wrap:wrap;border:1px dashed var(--border);border-radius:calc(var(--radius) + 4px);padding:12px 16px;margin-top:4px}
        .addstep select{flex:1 1 180px}
        .chainfoot{position:sticky;bottom:0;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;
        padding:14px 0;margin-top:12px;border-top:1px solid var(--border);background:var(--background)}
        .chain.done .step{margin-bottom:0}.chain.done .card .line{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
        .chain.done .card a{text-decoration:none}.chain.done .card a:hover{text-decoration:underline}
        """;
}
