using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace BackgroundJobs.Demo.Telemetry;

public sealed record SpanView(string Name, string Kind, string Status, string? StatusDescription, Dictionary<string, object?> Tags);

public sealed record MeasurementView(string Instrument, string? Unit, double Value, Dictionary<string, object?> Tags);

/// In-memory stand-in for an OpenTelemetry exporter: what the library emits, exposed at /demo/telemetry.
public sealed class TelemetryRecorder : IDisposable
{
    private readonly ActivityListener _activities;
    private readonly MeterListener _meters = new();
    private readonly List<SpanView> _spans = [];
    private readonly List<MeasurementView> _measurements = [];
    private readonly Lock _lock = new();

    public TelemetryRecorder()
    {
        _activities = new ActivityListener
        {
            ShouldListenTo = s => s.Name == BackgroundJobsTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                lock (_lock) _spans.Add(new SpanView(a.DisplayName, a.Kind.ToString(), a.Status.ToString(), a.StatusDescription,
                    a.TagObjects.ToDictionary(t => t.Key, t => t.Value)));
            },
        };
        ActivitySource.AddActivityListener(_activities);

        _meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == BackgroundJobsTelemetry.Name) listener.EnableMeasurementEvents(instrument);
        };
        _meters.SetMeasurementEventCallback<long>((i, v, tags, _) => Record(i, v, tags));
        _meters.SetMeasurementEventCallback<double>((i, v, tags, _) => Record(i, v, tags));
        _meters.Start();
    }

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var view = new MeasurementView(instrument.Name, instrument.Unit, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value));
        lock (_lock)
        {
            // Gauges: keep only the latest observation.
            if (instrument.IsObservable) _measurements.RemoveAll(m => m.Instrument == instrument.Name);
            _measurements.Add(view);
        }
    }

    public (List<SpanView> Spans, List<MeasurementView> Measurements) Snapshot()
    {
        _meters.RecordObservableInstruments();
        lock (_lock) return (_spans.ToList(), _measurements.ToList());
    }

    public void Dispose()
    {
        _activities.Dispose();
        _meters.Dispose();
    }
}
