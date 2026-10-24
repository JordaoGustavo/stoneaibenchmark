using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BackgroundJobs;

/// <summary>
/// Every per-job option of <see cref="JobOptions"/> (except the continuation fields, which the forms set themselves) as a
/// collapsible form section, with the server defaults as placeholders and the job type's registration settings as
/// read-only info. Used by the trigger form ("opt.") and by each chain step ("s{i}.opt.").
/// </summary>
public static partial class DashboardEndpoints
{
    private const string FieldErrorsKey = "BackgroundJobs.Dashboard.FieldErrors";
    private static readonly Regex QueueName = new("^[a-z0-9][a-z0-9_.-]{0,63}$", RegexOptions.CultureInvariant);

    private static Dictionary<string, string> FieldErrors(HttpContext http)
    {
        if (http.Items[FieldErrorsKey] is Dictionary<string, string> d) return d;
        d = new Dictionary<string, string>(StringComparer.Ordinal);
        http.Items[FieldErrorsKey] = d;
        return d;
    }

    private static BackgroundJobsOptions ServerDefaults(HttpContext http) =>
        http.RequestServices.GetService<IOptions<BackgroundJobsOptions>>()?.Value ?? new BackgroundJobsOptions();

    private static string OptionsFields(HttpContext http, string jobName, IFormCollection? posted, TimeZoneInfo tz, string prefix, string idPrefix)
    {
        var defaults = ServerDefaults(http);
        var fieldErrors = FieldErrors(http);
        string Value(string key) => E(posted?[prefix + key].LastOrDefault() ?? "");
        string Field(string key, string label, string control, string hint)
        {
            var error = fieldErrors.GetValueOrDefault(prefix + key);
            return $"<div class=\"field{(error is null ? "" : " bad")}\"><label for=\"{idPrefix}{key}\">{label}</label>{control}" +
                   (error is not null ? $"<p class=\"fielderr\">{E(error)}</p>" : hint.Length > 0 ? $"<p class=\"hint\">{hint}</p>" : "") + "</div>";
        }
        string Input(string key, string label, string attrs, string placeholder, string hint) =>
            Field(key, label, $"<input {attrs} id=\"{idPrefix}{key}\" name=\"{prefix}{key}\" value=\"{Value(key)}\" placeholder=\"{E(placeholder)}\">", hint);

        var backoff = posted?[prefix + "retryBackoff"].LastOrDefault() ?? "";
        string Option(string value, string label) => $"<option value=\"{value}\"{(backoff == value ? " selected" : "")}>{label}</option>";
        bool IsSet(string key) => (posted?[prefix + key].LastOrDefault()?.Trim().Length ?? 0) > 0;
        var set = posted?.Count(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal) && kv.Value.ToString().Trim().Length > 0) ?? 0;
        var hasErrors = fieldErrors.Keys.Any(k => k.StartsWith(prefix, StringComparison.Ordinal));
        // The less common options stay folded unless one of them was filled in or failed validation.
        string[] advanced = ["retryDelayMs", "retryJitterMs", "retryBackoff", "retryBackoffMultiplier", "maxRetryDelayMs", "idempotencyKey"];
        var advancedOpen = advanced.Any(k => IsSet(k) || fieldErrors.ContainsKey(prefix + k));
        var defaultBackoff = defaults.DefaultRetryBackoff == RetryBackoff.Exponential ? "exponential" : "fixed";
        var maxDelayDefault = defaults.DefaultMaxRetryDelay == Timeout.InfiniteTimeSpan ? "no cap" : Ms(defaults.DefaultMaxRetryDelay);

        // Placeholders show the server defaults, so hints only say what a field means.
        var sb = new StringBuilder("<details class=\"opts\"").Append(set > 0 || hasErrors ? " open" : "").Append("><summary>Options ")
            .Append(set > 0 ? $"<span class=\"muted\">· {set} set</span>" : "<span class=\"muted\">· when, where and how many tries; blank uses the defaults</span>").Append("</summary>")
            .Append("<div class=\"row\">")
            .Append(Input("runAt", "Run at", "type=\"datetime-local\" step=\"1\"", "", $"blank runs now · {E(DashboardForms.ZoneLabel(tz))}"))
            .Append(Input("queue", "Queue", "type=\"text\" class=\"mono\" autocomplete=\"off\" spellcheck=\"false\"", JobQueues.Default, ""))
            .Append(Input("priority", "Priority", $"type=\"number\" min=\"{JobQueues.MinPriority}\" max=\"{JobQueues.MaxPriority}\" step=\"1\"",
                JobQueues.DefaultPriority.ToString(CultureInfo.InvariantCulture), $"{JobQueues.MinPriority} to {JobQueues.MaxPriority}, higher first"))
            .Append(Input("maxAttempts", "Max attempts", "type=\"number\" min=\"1\" step=\"1\"", Num(defaults.DefaultMaxAttempts), "includes the first run"))
            .Append("</div><details class=\"more\"").Append(advancedOpen ? " open" : "").Append("><summary>Advanced <span class=\"muted\">· retry timing, deduplication</span></summary><div class=\"row\">")
            .Append(Input("retryDelayMs", "Retry delay (ms)", "type=\"number\" min=\"0\" step=\"1\"", Ms(defaults.DefaultRetryDelay), ""))
            .Append(Input("retryJitterMs", "Retry jitter (ms)", "type=\"number\" min=\"0\" step=\"1\"", Ms(defaults.DefaultRetryJitter), "random extra delay"))
            .Append(Field("retryBackoff", "Retry backoff", $"<select id=\"{idPrefix}retryBackoff\" name=\"{prefix}retryBackoff\">{Option("", $"Default ({defaultBackoff})")}{Option("fixed", "Fixed")}{Option("exponential", "Exponential")}</select>", ""))
            .Append(Input("retryBackoffMultiplier", "Backoff multiplier", "type=\"number\" min=\"1\" max=\"100\" step=\"any\"",
                defaults.DefaultRetryBackoffMultiplier.ToString(CultureInfo.InvariantCulture), "exponential only"))
            .Append(Input("maxRetryDelayMs", "Max retry delay (ms)", "type=\"number\" min=\"0\" step=\"1\"", maxDelayDefault, "caps the backoff"))
            .Append(Input("idempotencyKey", "Idempotency key", "type=\"text\" class=\"mono\" maxlength=\"200\" autocomplete=\"off\"", "",
                "skip if a job with this key is unfinished"))
            .Append("</div></details>").Append(RegistrationInfo(http, jobName, defaults)).Append("</details>");
        return sb.ToString();
    }

    /// <summary>
    /// Settings fixed when the job type was registered; shown so the per-job options are read in context.
    /// </summary>
    private static string RegistrationInfo(HttpContext http, string jobName, BackgroundJobsOptions defaults)
    {
        var reg = http.RequestServices.GetService<JobRegistry>()?.GetOptions(jobName);
        if (reg is null) return "";
        var timeout = reg.Timeout is { } t ? Duration(t)
            : defaults.DefaultJobTimeout == Timeout.InfiniteTimeSpan ? "none" : $"{Duration(defaults.DefaultJobTimeout)} (server default)";
        var breaker = reg.CircuitBreaker is { } cb ? $"opens after {cb.FailureThreshold} failures, for {Duration(cb.BreakDuration)}" : "off";
        return "<p class=\"reginfo\"><span>Fixed for this job type:</span> " + string.Join(" · ",
            $"timeout <b>{timeout}</b>", $"concurrency <b>{(reg.MaxConcurrency is { } m ? $"{m} in the cluster" : "unlimited")}</b>",
            $"circuit breaker <b>{breaker}</b>", $"args v<b>{Num(reg.ArgsVersion)}</b>") + "</p>";
    }

    private static string Num(int n) => n.ToString(CultureInfo.InvariantCulture);
    private static string Ms(TimeSpan t) => ((long)t.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Reads the options under <paramref name="prefix"/>; each problem goes to <paramref name="errors"/> and next to its field.
    /// Null when nothing was set.
    /// </summary>
    private static JobOptions? ParseOptions(HttpContext http, IFormCollection form, TimeZoneInfo tz, List<string> errors, string prefix)
    {
        var fieldErrors = FieldErrors(http);
        string? Get(string key) => form[prefix + key].LastOrDefault() is { } v && v.Trim().Length > 0 ? v.Trim() : null;
        void Fail(string key, string message)
        {
            errors.Add(message);
            fieldErrors[prefix + key] = message;
        }
        int? Whole(string key, string label, int min, int max = int.MaxValue, string? message = null)
        {
            if (Get(key) is not { } raw) return null;
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n >= min && n <= max) return n;
            Fail(key, message ?? (max == int.MaxValue ? $"{label} must be a whole number of at least {min}." : $"{label} must be a whole number from {min} to {max}."));
            return null;
        }
        TimeSpan? Millis(string key, string label) =>
            Whole(key, label, 0, message: $"{label} must be a whole number of milliseconds, 0 or more.") is { } ms ? TimeSpan.FromMilliseconds(ms) : null;

        DateTimeOffset? runAt = null;
        if (Get("runAt") is { } r)
        {
            if (!DateTime.TryParse(r, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d)) Fail("runAt", "Run at: a date and time expected, e.g. 2030-01-31 09:00.");
            else
            {
                try { runAt = new DateTimeOffset(DashboardForms.ToUtc(d, tz), TimeSpan.Zero); }
                catch (ArgumentException) { Fail("runAt", $"Run at: {r} does not exist in {tz.Id} (daylight-saving change); pick a time outside the skipped hour."); }
            }
        }
        var maxAttempts = Whole("maxAttempts", "Max attempts", 1, message: "Max attempts must be a whole number of at least 1.");
        var retryDelay = Millis("retryDelayMs", "Retry delay");
        var retryJitter = Millis("retryJitterMs", "Retry jitter");
        RetryBackoff? backoff = null;
        switch (Get("retryBackoff"))
        {
            case null: break;
            case "fixed": backoff = RetryBackoff.Fixed; break;
            case "exponential": backoff = RetryBackoff.Exponential; break;
            case var other: Fail("retryBackoff", $"Retry backoff \"{other}\" is unknown; pick fixed or exponential."); break;
        }
        double? multiplier = null;
        if (Get("retryBackoffMultiplier") is { } mult)
        {
            if (double.TryParse(mult, NumberStyles.Float, CultureInfo.InvariantCulture, out var f) && double.IsFinite(f) && f >= 1 && f <= 100) multiplier = f;
            else Fail("retryBackoffMultiplier", "Backoff multiplier must be a number from 1 to 100.");
        }
        var maxRetryDelay = Millis("maxRetryDelayMs", "Max retry delay");
        var queue = Get("queue");
        if (queue != null && !QueueName.IsMatch(queue))
            Fail("queue", $"Queue \"{queue}\" is not a valid name: lowercase letters, digits, '_', '.' or '-', starting with a letter or digit, up to 64 characters.");
        var priority = Whole("priority", "Priority", JobQueues.MinPriority, JobQueues.MaxPriority);
        var key = Get("idempotencyKey");
        if (key is { Length: > 200 }) Fail("idempotencyKey", $"Idempotency key has {key.Length} characters; the maximum is 200.");

        if (runAt is null && maxAttempts is null && retryDelay is null && retryJitter is null && backoff is null && multiplier is null
            && maxRetryDelay is null && queue is null && priority is null && key is null)
            return null;
        return new JobOptions
        {
            RunAt = runAt, MaxAttempts = maxAttempts, RetryDelay = retryDelay, RetryJitter = retryJitter,
            RetryBackoff = backoff, RetryBackoffMultiplier = multiplier, MaxRetryDelay = maxRetryDelay,
            Queue = queue, Priority = priority, IdempotencyKey = key,
        };
    }

    private const string OptionsCss = """
        .opts .row{margin-top:10px}.opts .hint{font-size:11px}
        .more{margin-top:12px}.more summary{cursor:pointer;font-weight:500;font-size:13px}
        .field.bad input,.field.bad select{border-color:var(--destructive)}.fielderr{margin:0;font-size:12px;color:var(--destructive)}
        .reginfo{margin:12px 0 0;font-size:12px;color:var(--muted-foreground)}.reginfo b{font-weight:500;color:var(--foreground)}
        """;
}
