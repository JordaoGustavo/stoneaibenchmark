namespace BackgroundJobs;

/// <summary>
/// One job of a batch enqueue (<see cref="IJobClient.EnqueueManyAsync(IEnumerable{JobToEnqueue}, CancellationToken)"/>):
/// the same name, args and options <see cref="IJobClient.EnqueueAsync{TArgs}"/> takes. <paramref name="Args"/> is
/// serialized by its runtime type and, for a registered name, must be of the registered args type (or a
/// <c>JsonElement</c>/<c>JsonNode</c> of its shape).
/// </summary>
/// <param name="JobName">Job name.</param>
/// <param name="Args">Job args.</param>
/// <param name="Options">Per-job options; null uses the defaults.</param>
public sealed record JobToEnqueue(string JobName, object? Args, JobOptions? Options = null);
