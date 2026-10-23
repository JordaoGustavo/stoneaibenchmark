using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace BackgroundJobs.E2E.Infra;

public sealed record JobView(
    string Id, string Name, JsonElement Args, string State, int Attempts, int MaxAttempts, int RetryDelayMs,
    DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
    List<HistoryView> History, List<AttemptView> AttemptHistory, string Queue, int Priority, string? IdempotencyKey = null,
    string? ContinueAfter = null, string? ContinuationCondition = null, int RetryJitterMs = 0, string RetryBackoff = "fixed",
    double RetryBackoffMultiplier = 1, int? MaxRetryDelayMs = null);

public sealed record HistoryView(string State, DateTime At, string Reason);

public sealed record AttemptView(int Attempt, DateTime StartedAt, DateTime? FinishedAt, string? Outcome, string? Error);

public sealed record EventView(string Type, string JobId, int Attempt, DateTime At, JsonElement? Detail);

public sealed class ApiClient(Uri baseAddress)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _http = new() { BaseAddress = baseAddress, Timeout = TimeSpan.FromSeconds(10) };

    public Uri BaseAddress => baseAddress;

    public async Task<bool> IsHealthyAsync()
    {
        try { return (await _http.GetAsync("/health")).StatusCode == HttpStatusCode.OK; }
        catch (HttpRequestException) { return false; }
    }

    public Task<HttpResponseMessage> GetHealthAsync() => _http.GetAsync("/health");

    public Task<HttpResponseMessage> PostJobRawAsync(object body) => _http.PostAsJsonAsync("/demo/jobs", body, Json);

    public async Task<string> EnqueueAsync(string job, object @params, object? options = null)
    {
        var res = await PostJobRawAsync(new { job, @params, options });
        Assert.Equal(HttpStatusCode.Accepted, res.StatusCode);
        var body = await res.Content.ReadFromJsonAsync<JsonObject>(Json);
        return body!["jobId"]!.GetValue<string>();
    }

    public Task<HttpResponseMessage> PostRawAsync(string path, object body) => _http.PostAsJsonAsync(path, body, Json);

    /// The jobIds of a 2xx response from /demo/jobs/batch or /demo/orders.
    public static async Task<List<string>> JobIdsAsync(HttpResponseMessage response)
    {
        Assert.True(response.IsSuccessStatusCode, await response.Content.ReadAsStringAsync());
        var body = await response.Content.ReadFromJsonAsync<JsonObject>(Json);
        return body!["jobIds"]!.AsArray().Select(id => id!.GetValue<string>()).ToList();
    }

    public async Task<JobView> GetJobAsync(string id)
    {
        var res = await _http.GetAsync($"/api/jobs/{id}");
        Assert.Equal(HttpStatusCode.OK, res.StatusCode);
        return (await res.Content.ReadFromJsonAsync<JobView>(Json))!;
    }

    public Task<HttpResponseMessage> GetJobRawAsync(string id) => _http.GetAsync($"/api/jobs/{id}");

    public Task<HttpResponseMessage> ListRawAsync(string query = "") => _http.GetAsync("/api/jobs" + query);

    public Task<HttpResponseMessage> GetRawAsync(string path) => _http.GetAsync(path);

    public Task<HttpResponseMessage> DeleteAsync(string id) => _http.DeleteAsync($"/api/jobs/{id}");

    public Task<HttpResponseMessage> RequeueAsync(string id) => _http.PostAsync($"/api/jobs/{id}/requeue", null);

    public Task<HttpResponseMessage> CancelAsync(string id) => _http.PostAsync($"/api/jobs/{id}/cancel", null);

    public async Task<List<EventView>> EventsAsync()
    {
        var body = await _http.GetFromJsonAsync<JsonObject>("/demo/events", Json);
        return body!["items"].Deserialize<List<EventView>>(Json)!;
    }

    public async Task<JobView> WaitForStateAsync(string id, string state, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        JobView job;
        do
        {
            job = await GetJobAsync(id);
            if (job.State == state) return job;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException($"job {id} stayed in {job.State}, expected {state}");
    }

    public async Task<List<EventView>> WaitForEventsAsync(Func<List<EventView>, bool> until, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        List<EventView> events;
        do
        {
            events = await EventsAsync();
            if (until(events)) return events;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        throw new TimeoutException("events condition not met: " + JsonSerializer.Serialize(events, Json));
    }
}
