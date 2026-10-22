namespace BackgroundJobs.Processing;

/// Opt-in, in-memory, per job type. Open: the dispatcher does not claim the type.
/// After the break it lets exactly one trial job through (half-open) before closing again.
internal sealed class JobCircuitBreaker(JobRegistry registry, TimeProvider time)
{
    private enum Phase { Closed, Open, HalfOpenIdle, HalfOpenTrial }

    private sealed class State
    {
        public Phase Phase;
        public int Failures;
        public DateTimeOffset OpenUntil;
    }

    private readonly Dictionary<string, State> _states = [];
    private readonly Lock _lock = new();

    /// Job names the dispatcher must not claim right now.
    public IReadOnlyCollection<string> BlockedNames()
    {
        var now = time.GetUtcNow();
        lock (_lock)
        {
            var blocked = new List<string>();
            foreach (var (name, s) in _states)
            {
                if (s.Phase == Phase.Open && now >= s.OpenUntil) s.Phase = Phase.HalfOpenIdle;
                if (s.Phase is Phase.Open or Phase.HalfOpenTrial) blocked.Add(name);
            }
            return blocked;
        }
    }

    /// Called serially by the dispatcher right after a claim, so only one half-open trial is ever taken.
    public void OnClaimed(string name)
    {
        lock (_lock)
            if (_states.TryGetValue(name, out var s) && s.Phase == Phase.HalfOpenIdle) s.Phase = Phase.HalfOpenTrial;
    }

    public void OnFinished(string name, bool success)
    {
        var options = registry.Find(name)?.Options.CircuitBreaker;
        if (options == null) return;
        lock (_lock)
        {
            if (!_states.TryGetValue(name, out var s)) _states[name] = s = new State();
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
