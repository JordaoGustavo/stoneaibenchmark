using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;

namespace BackgroundJobs;

/// <summary>
/// Derives an HTML form from a job's argument type and turns the posted form back into the JSON the job will receive.
/// Scalars get native inputs; anything else (collections, nested objects) falls back to a JSON textarea.
/// </summary>
internal static class DashboardForms
{
    private const string ArgPrefix = "arg.";

    private enum Kind { Text, Integer, Decimal, Bool, Enum, DateTime, Guid, TimeSpan, Json }

    private sealed record Field(string JsonName, string Label, Type Type, Kind Kind, bool Required, object? Default);

    public static string Render(Type argsType, IFormCollection? posted, TimeZoneInfo tz)
    {
        var fields = FieldsOf(argsType);
        var sb = new StringBuilder();
        if (fields.Count == 0) sb.Append("<p class=\"muted\">This job takes no arguments.</p>");
        foreach (var f in fields) sb.Append(RenderField(f, posted, tz));
        return sb.ToString();
    }

    /// <summary>
    /// Builds the args object from the posted form and checks it deserializes into the job's argument type.
    /// </summary>
    public static JsonObject Parse(Type argsType, IFormCollection form, TimeZoneInfo tz, List<string> errors)
    {
        var result = new JsonObject();
        foreach (var f in FieldsOf(argsType))
        {
            var raw = form[ArgPrefix + f.JsonName].LastOrDefault()?.Trim() ?? "";
            if (raw.Length == 0)
            {
                if (f.Kind == Kind.Bool) result[f.JsonName] = false;
                else if (f.Required) errors.Add($"{f.Label} is required.");
                continue;
            }
            try { result[f.JsonName] = ToNode(f, raw, tz); }
            catch (Exception e) when (e is FormatException or OverflowException or JsonException or ArgumentException)
            {
                errors.Add($"{f.Label}: {Describe(f.Kind)} expected.");
            }
        }
        if (errors.Count == 0)
        {
            try { result.Deserialize(argsType, JobJson.Options); }
            catch (JsonException e) { errors.Add($"Arguments do not match {argsType.Name}: {e.Message}"); }
        }
        return result;
    }

    private static JsonNode? ToNode(Field f, string raw, TimeZoneInfo tz) => f.Kind switch
    {
        Kind.Integer => JsonValue.Create(long.Parse(raw, CultureInfo.InvariantCulture)),
        Kind.Decimal => JsonValue.Create(decimal.Parse(raw, NumberStyles.Float, CultureInfo.InvariantCulture)),
        Kind.Bool => JsonValue.Create(raw == "true"),
        // Enums are stored as their numeric value: JobJson has no string-enum converter.
        Kind.Enum => JsonValue.Create(long.Parse(raw, CultureInfo.InvariantCulture)),
        Kind.DateTime => JsonValue.Create(ToUtc(DateTime.Parse(raw, CultureInfo.InvariantCulture), tz)),
        Kind.Guid => JsonValue.Create(Guid.Parse(raw)),
        Kind.TimeSpan => JsonValue.Create(TimeSpan.Parse(raw, CultureInfo.InvariantCulture).ToString("c")),
        Kind.Json => JsonNode.Parse(raw),
        _ => JsonValue.Create(raw),
    };

    /// <summary>
    /// datetime-local inputs carry a wall-clock time with no offset; it is read in the viewer's time zone.
    /// Throws ArgumentException for a time skipped by a daylight-saving jump.
    /// </summary>
    public static DateTime ToUtc(DateTime wallClock, TimeZoneInfo tz) =>
        TimeZoneInfo.ConvertTimeToUtc(DateTime.SpecifyKind(wallClock, DateTimeKind.Unspecified), tz);

    public static string ZoneLabel(TimeZoneInfo tz) => tz.Id;

    private static string RenderField(Field f, IFormCollection? posted, TimeZoneInfo tz)
    {
        var name = ArgPrefix + f.JsonName;
        var value = posted?[name].LastOrDefault() ?? FormatDefault(f, tz);
        var id = "f-" + f.JsonName;
        var req = f.Required ? " required" : "";
        var sb = new StringBuilder("<div class=\"field\">");

        if (f.Kind == Kind.Bool)
        {
            var on = posted != null ? value == "true" : f.Default is true;
            // The hidden input submits "false" when the box is unchecked; the checkbox, coming later, wins when checked.
            sb.Append("<label class=\"check\"><input type=\"hidden\" name=\"").Append(name).Append("\" value=\"false\">")
              .Append("<input type=\"checkbox\" id=\"").Append(id).Append("\" name=\"").Append(name).Append("\" value=\"true\"")
              .Append(on ? " checked" : "").Append("> ").Append(E(f.Label)).Append("</label>");
            return sb.Append(Hint(f, tz)).Append("</div>").ToString();
        }

        sb.Append("<label for=\"").Append(id).Append("\">").Append(E(f.Label))
          .Append(f.Required ? "" : " <span class=\"muted\">(optional)</span>").Append("</label>");
        var v = E(value ?? "");
        switch (f.Kind)
        {
            case Kind.Enum:
                sb.Append("<select id=\"").Append(id).Append("\" name=\"").Append(name).Append('"').Append(req).Append('>');
                if (!f.Required) sb.Append("<option value=\"\"></option>");
                foreach (var member in Enum.GetValues(Nullable.GetUnderlyingType(f.Type) ?? f.Type))
                {
                    var num = Convert.ToInt64(member, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
                    sb.Append("<option value=\"").Append(num).Append('"').Append(num == value ? " selected" : "").Append('>')
                      .Append(E(member.ToString()!)).Append("</option>");
                }
                sb.Append("</select>");
                break;
            case Kind.Json:
                sb.Append("<textarea class=\"mono\" rows=\"4\" id=\"").Append(id).Append("\" name=\"").Append(name).Append('"').Append(req)
                  .Append(" placeholder=\"JSON\">").Append(v).Append("</textarea>");
                break;
            default:
                var (type, extra) = f.Kind switch
                {
                    Kind.Integer => ("number", " step=\"1\""),
                    Kind.Decimal => ("number", " step=\"any\""),
                    Kind.DateTime => ("datetime-local", " step=\"1\""),
                    Kind.Guid => ("text", " class=\"mono\" placeholder=\"00000000-0000-0000-0000-000000000000\""),
                    Kind.TimeSpan => ("text", " class=\"mono\" placeholder=\"hh:mm:ss\""),
                    _ => ("text", ""),
                };
                sb.Append("<input type=\"").Append(type).Append("\" id=\"").Append(id).Append("\" name=\"").Append(name)
                  .Append("\" value=\"").Append(v).Append('"').Append(extra).Append(req).Append('>');
                break;
        }
        return sb.Append(Hint(f, tz)).Append("</div>").ToString();
    }

    private static string Hint(Field f, TimeZoneInfo tz)
    {
        var t = Nullable.GetUnderlyingType(f.Type) ?? f.Type;
        var suffix = f.Kind == Kind.DateTime ? $" · {ZoneLabel(tz)}" : "";
        return $"<p class=\"hint mono\">{E(f.JsonName)}: {E(TypeName(t))}{suffix}</p>";
    }

    private static string? FormatDefault(Field f, TimeZoneInfo tz) => f.Default switch
    {
        null => null,
        DateTime d => TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(d, DateTimeKind.Utc), tz).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture),
        DateTimeOffset d => TimeZoneInfo.ConvertTimeFromUtc(d.UtcDateTime, tz).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture),
        Enum e => Convert.ToInt64(e, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        IFormattable x => x.ToString(null, CultureInfo.InvariantCulture),
        var x => x.ToString(),
    };

    /// <summary>
    /// Records and other immutable types are described by their widest constructor; mutable types by settable properties.
    /// </summary>
    private static List<Field> FieldsOf(Type argsType)
    {
        var nullability = new NullabilityInfoContext();
        var naming = JobJson.Options.PropertyNamingPolicy;
        string Json(string n) => naming?.ConvertName(n) ?? n;

        var ctor = argsType.GetConstructors().OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();
        if (ctor != null && ctor.GetParameters().Length > 0)
            return ctor.GetParameters().Select(p => new Field(
                Json(p.Name!), Humanize(p.Name!), p.ParameterType, KindOf(p.ParameterType),
                Required: !p.HasDefaultValue && !IsNullable(p.ParameterType, nullability.Create(p)) && KindOf(p.ParameterType) != Kind.Bool,
                Default: p.HasDefaultValue ? p.DefaultValue : null)).ToList();

        return argsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.SetMethod!.IsPublic)
            .Select(p => new Field(
                Json(p.Name), Humanize(p.Name), p.PropertyType, KindOf(p.PropertyType),
                Required: p.GetCustomAttribute<System.Runtime.CompilerServices.RequiredMemberAttribute>() != null,
                Default: null)).ToList();
    }

    private static bool IsNullable(Type t, NullabilityInfo info) =>
        Nullable.GetUnderlyingType(t) != null || (!t.IsValueType && info.WriteState == NullabilityState.Nullable);

    private static Kind KindOf(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (t == typeof(string) || t == typeof(char)) return Kind.Text;
        if (t == typeof(bool)) return Kind.Bool;
        if (t.IsEnum) return Kind.Enum;
        if (t == typeof(byte) || t == typeof(short) || t == typeof(int) || t == typeof(long)
            || t == typeof(sbyte) || t == typeof(ushort) || t == typeof(uint) || t == typeof(ulong)) return Kind.Integer;
        if (t == typeof(float) || t == typeof(double) || t == typeof(decimal)) return Kind.Decimal;
        if (t == typeof(DateTime) || t == typeof(DateTimeOffset)) return Kind.DateTime;
        if (t == typeof(Guid)) return Kind.Guid;
        if (t == typeof(TimeSpan)) return Kind.TimeSpan;
        return Kind.Json;
    }

    private static string Describe(Kind k) => k switch
    {
        Kind.Integer => "a whole number",
        Kind.Decimal or Kind.Enum => "a number",
        Kind.DateTime => "a date and time",
        Kind.Guid => "a GUID",
        Kind.TimeSpan => "a duration like 00:00:05",
        Kind.Json => "valid JSON",
        _ => "a value",
    };

    private static string TypeName(Type t) => t.IsGenericType
        ? $"{t.Name[..t.Name.IndexOf('`')]}<{string.Join(", ", t.GetGenericArguments().Select(TypeName))}>"
        : t.Name;

    /// <summary>
    /// "DelayMs" → "Delay ms".
    /// </summary>
    private static string Humanize(string name)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (i > 0 && char.IsUpper(c) && !char.IsUpper(name[i - 1])) sb.Append(' ').Append(char.ToLowerInvariant(c));
            else sb.Append(i == 0 ? char.ToUpperInvariant(c) : c);
        }
        return sb.ToString();
    }

    private static string E(string s) => WebUtility.HtmlEncode(s);
}
