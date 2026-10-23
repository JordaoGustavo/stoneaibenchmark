namespace BackgroundJobs.Processing;

internal sealed record CircuitBreakerStatus(string Job, string State, int ConsecutiveFailures, DateTimeOffset? OpenUntil);

/// What one claim of one queue may take. Blocked: names it must skip. Trials: half-open names reserved for this claim
/// alone; the dispatcher claims them one job at a time, so a batch cannot take several trials of the same type.
internal sealed record ClaimPermit(IReadOnlyCollection<string> Blocked, IReadOnlyCollection<string> Trials);

/// Opt-in, in-memory, per job type. Open: the dispatcher does not claim the type.
/// After the break it lets exactly one trial job through (half-open) before closing again. Queues claim concurrently,
/// so the trial is reserved for one claim of one queue at a time; every other claim skips the type meanwhile. A claim
/// that does not find the type hands the reservation back, so the queues take turns until one has a job of the type.
internal sealed class JobCircuitBreaker(JobRegistry registry, TimeProvider time)
{
    private enum Phase { Closed, Open, HalfOpenIdle, HalfOpenReserved, HalfOpenTrial }

    private sealed class State
    {
        public Phase Phase;
        public int Failures;
        public DateTimeOffset OpenUntil;
    }

    private readonly Dictionary<string, State> _states = [];
    private readonly Lock _lock = new();

    /// Call before a claim, then <see cref="EndClaim"/> with what it claimed.
    public ClaimPermit BeginClaim()
    {
        var now = time.GetUtcNow();
        lock (_lock)
        {
            List<string> blocked = [], trials = [];
            foreach (var (name, s) in _states)
            {
                if (s.Phase == Phase.Open && now >= s.OpenUntil) s.Phase = Phase.HalfOpenIdle;
                if (s.Phase == Phase.HalfOpenIdle)
                {
                    s.Phase = Phase.HalfOpenReserved;
                    trials.Add(name);
                }
                else if (s.Phase != Phase.Closed) blocked.Add(name);
            }
            return new ClaimPermit(blocked, trials);
        }
    }

    /// A reserved trial becomes the trial when its type was claimed, and is offered again otherwise.
    public void EndClaim(ClaimPermit permit, IEnumerable<string> claimedNames)
    {
        var claimed = claimedNames.ToHashSet();
        lock (_lock)
            foreach (var name in permit.Trials)
                if (_states.TryGetValue(name, out var s) && s.Phase == Phase.HalfOpenReserved)
                    s.Phase = claimed.Contains(name) ? Phase.HalfOpenTrial : Phase.HalfOpenIdle;
    }

    /// Every job type with a breaker configured, closed ones included, ordered by name.
    public IReadOnlyList<CircuitBreakerStatus> Snapshot()
    {
        var now = time.GetUtcNow();
        lock (_lock)
            return registry.All.Where(r => r.Options.CircuitBreaker != null).OrderBy(r => r.Name, StringComparer.Ordinal).Select(r =>
            {
                if (!_states.TryGetValue(r.Name, out var s)) return new CircuitBreakerStatus(r.Name, "closed", 0, null);
                return s.Phase switch
                {
                    Phase.Open when now < s.OpenUntil => new CircuitBreakerStatus(r.Name, "open", s.Failures, s.OpenUntil),
                    Phase.Closed => new CircuitBreakerStatus(r.Name, "closed", s.Failures, null),
                    _ => new CircuitBreakerStatus(r.Name, "half-open", s.Failures, null),
                };
            }).ToList();
    }

    public void OnFinished(string name, bool success)
    {
        var options = registry.Find(name)?.Options.CircuitBreaker;
        if (options == null) return;
        lock (_lock)
        {
            if (!_states.TryGetValue(name, out var s)) _states[name] = s = new State();
            // A late success from a job claimed before the breaker opened must not skip the half-open trial.
            if (success && s.Phase == Phase.Open) return;
            if (success)
            {
                s.Phase = Phase.Closed;
                s.Failures = 0;
                return;
            }
            s.Failures++;
            if (s.Phase == Phase.HalfOpenTrial || s.Failures >= options.FailureThreshold)
            {
                s.Phase = Phase.Open;
                s.OpenUntil = time.GetUtcNow() + options.BreakDuration;
            }
        }
    }
}
