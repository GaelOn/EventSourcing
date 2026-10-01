using System.Collections.Immutable;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using EventSourcing;
using EventSourcing.Handlers;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

if (args.SequenceEqual(new[] { "--verify" }))
{
    foreach (var history in new[] { 100, 10000 })
    foreach (var position in new[] { 10, 90 })
    foreach (var suffix in new[] { 1, 100 })
    foreach (var participants in new[] { 2, 8 })
        new StreamBenchmarks { HistoricalEventCount = history, SnapshotPositionPercent = position,
            SuffixLength = suffix, ParticipantCount = participants }.Setup();
    Console.WriteLine("All 16 parameter combinations verified outside timed operations.");
}
else BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, BaselineConfig.Create());

public sealed record Balance(decimal Amount);
public sealed class AmountEvent(Guid id, decimal amount, EventMetadata? metadata = null)
    : EventBase<AmountEvent>(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public override AmountEvent Clone(EventMetadata metadata) => new(Id, Amount, metadata);
}
public sealed class AmountHandler : IEventHandler<Balance, AmountEvent>
{
    public Balance Do(Balance s, AmountEvent e) => new(s.Amount + e.Amount);
    public Balance Undo(Balance s, AmountEvent e) => new(s.Amount - e.Amount);
}

[MemoryDiagnoser]
public class StreamBenchmarks
{
    public IEnumerable<int> HistoryCounts => Baseline ? new[] { 10000 } : new[] { 100, 10000 };
    public IEnumerable<int> SuffixLengths => Baseline ? new[] { 1 } : new[] { 1, 100 };
    public IEnumerable<int> ParticipantCounts => Baseline ? new[] { 2 } : new[] { 2, 8 };
    private static bool Baseline => Environment.GetEnvironmentVariable("EVENTSOURCING_BENCHMARK_PROFILE") == "baseline";
    [ParamsSource(nameof(HistoryCounts))] public int HistoricalEventCount { get; set; }
    // Percent of historical events covered by the checkpoint: older versus recent.
    [Params(10, 90)] public int SnapshotPositionPercent { get; set; }
    [ParamsSource(nameof(SuffixLengths))] public int SuffixLength { get; set; }
    [ParamsSource(nameof(ParticipantCounts))] public int ParticipantCount { get; set; }
    private readonly Balance _dispatchOpening = new(0);
    private ReplayDefinition<Balance> _definition = null!;
    private EventStream<Balance, AmountEvent> _stream = null!;
    private EventReducer<Balance, AmountEvent> _rules = null!;
    private EventHandlerDispatcher<Balance, AmountEvent> _dispatcher = null!;
    private InMemoryEventStreamStore<AmountEvent> _store = null!;
    private InMemoryEventStreamStore<AmountEvent> _writeStore = null!;
    private EventStream<Balance, AmountEvent>[] _participants = null!;
    private IReadOnlyList<AmountEvent> _history = null!;
    private AggregateSnapshot<Balance> _snapshot = null!;
    private StreamSlice<AmountEvent> _slice = null!;
    private AmountEvent[] _newEvents = null!;
    private Guid[] _prefixIds = null!;
    private EventStream<Balance, AmountEvent>? _lastApplied;
    private IReadOnlyList<EventStream<Balance, AmountEvent>>? _lastBatch;

    [GlobalSetup]
    public void Setup()
    {
        _rules = new("balance/v1", (s, e) => new(s.Amount + e.Amount));
        _dispatcher = EventHandlerDispatcher<Balance, AmountEvent>.CreateBuilder("balance/v1")
            .Register<AmountEvent>(new AmountHandler()).Build();
        _definition = new(new(0), _rules.RuleVersion);
        _store = new();
        var id = Guid.NewGuid();
        var stream = new EventStream<Balance, AmountEvent>(id, new(0), _rules);
        var cursor = HistoricalEventCount * SnapshotPositionPercent / 100;
        for (var i = 0; i < HistoricalEventCount + SuffixLength; i++)
        {
            stream = stream.Apply(new AmountEvent(Guid.NewGuid(), 1));
            if (i + 1 == cursor)
            {
                stream = stream.SaveAsync(_store).GetAwaiter().GetResult();
                _snapshot = stream.CaptureSnapshot(1);
            }
        }
        _stream = stream.SaveAsync(_store).GetAwaiter().GetResult();
        _history = _store.ReadAsync(id).GetAwaiter().GetResult();
        _slice = _store.ReadFromAsync(id, cursor).GetAwaiter().GetResult();
        _prefixIds = _history.Take(cursor).Select(e => e.Id).ToArray();
        _newEvents = Enumerable.Range(0, SuffixLength).Select(_ => new AmountEvent(Guid.NewGuid(), 1)).ToArray();
        _participants = Enumerable.Range(0, ParticipantCount).Select(_ =>
        {
            var participantId = Guid.NewGuid();
            var history = _history.Select(e => new AmountEvent(e.Id, e.Amount,
                new EventMetadata(e.Metadata.Version, participantId))).ToArray();
            return EventStream<Balance, AmountEvent>.Replay(participantId,
                new ReplayDefinition<Balance>(new(0), _rules.RuleVersion), _rules, history);
        }).ToArray();
        Check(FullReplay(), HistoricalEventCount + SuffixLength);
        Check(SnapshotRestore(), HistoricalEventCount + SuffixLength);
        Check(SnapshotLoadIncludingHistoricalIds().GetAwaiter().GetResult(), HistoricalEventCount + SuffixLength);
        Check(Apply(), HistoricalEventCount + 2 * SuffixLength);
        if (BuildHistoricalIdentityIndex().Count != cursor) throw new InvalidOperationException("Prefix ID index mismatch.");
        if (LoadHistoricalIds().PriorEventIds.Count != cursor) throw new InvalidOperationException("Slice identity mismatch.");
        if (Dispatch() != new Balance(1)) throw new InvalidOperationException("Dispatch mismatch.");
        ResetWrites();
        Check(ApplyAndSave().GetAwaiter().GetResult(), HistoricalEventCount + 2 * SuffixLength);
        ResetWrites();
        AtomicMultiStream().GetAwaiter().GetResult();
        Validate();
    }

    [IterationSetup(Targets = new[] { nameof(ApplyAndSave), nameof(AtomicMultiStream) })]
    public void ResetWrites()
    {
        _writeStore = new();
        _writeStore.AppendAsync(_stream.Id, 0, _history).GetAwaiter().GetResult();
        _writeStore.AppendBatchAsync(_participants.Select(s =>
            new StreamAppend<AmountEvent>(s.Id, 0, s.Events)).ToArray()).GetAwaiter().GetResult();
        _lastApplied = null; _lastBatch = null;
    }
    [IterationCleanup(Targets = new[] { nameof(ApplyAndSave), nameof(AtomicMultiStream) })]
    public void Validate()
    {
        if (_lastApplied is not null)
        {
            Check(_lastApplied, HistoricalEventCount + 2 * SuffixLength);
            if (_writeStore.ReadAsync(_stream.Id).GetAwaiter().GetResult().Count != _lastApplied.Version)
                throw new InvalidOperationException("Write mismatch.");
        }
        if (_lastBatch is not null)
            foreach (var s in _lastBatch)
            {
                Check(s, HistoricalEventCount + 2 * SuffixLength);
                if (_writeStore.ReadAsync(s.Id).GetAwaiter().GetResult().Count != HistoricalEventCount + 2 * SuffixLength)
                    throw new InvalidOperationException("Atomic write mismatch.");
            }
    }
    private static void Check(EventStream<Balance, AmountEvent> s, int count)
    { if (s.State.Amount != count || s.Version != count) throw new InvalidOperationException("Workload mismatch."); }

    [Benchmark] public EventStream<Balance, AmountEvent> Apply()
    {
        var s = _stream;
        foreach (var e in _newEvents) s = s.Apply(e);
        return s;
    }
    [Benchmark] public async Task<EventStream<Balance, AmountEvent>> ApplyAndSave()
    { _lastApplied = await Apply().SaveAsync(_writeStore); return _lastApplied; }
    [Benchmark] public EventStream<Balance, AmountEvent> FullReplay() => EventStream<Balance, AmountEvent>.Replay(
        _stream.Id, _definition, _rules, _history);
    [Benchmark] public EventStream<Balance, AmountEvent> SnapshotRestore() =>
        EventStream<Balance, AmountEvent>.Replay(_snapshot, 1, _rules, _slice);
    [Benchmark] public async Task<EventStream<Balance, AmountEvent>> SnapshotLoadIncludingHistoricalIds() =>
        EventStream<Balance, AmountEvent>.Replay(_snapshot, 1, _rules, await _store.ReadFromAsync(_stream.Id, _snapshot.StreamVersion));
    [Benchmark] public ImmutableHashSet<Guid> BuildHistoricalIdentityIndex() => _prefixIds.ToImmutableHashSet();
    [Benchmark] public StreamSlice<AmountEvent> LoadHistoricalIds() => new(_stream.Id,
        _snapshot.StreamVersion, _stream.Version, _slice.Events, _prefixIds);
    [Benchmark] public Balance Dispatch() => _dispatcher.Do(_dispatchOpening, _newEvents[0]);
    [Benchmark] public async Task<IReadOnlyList<EventStream<Balance, AmountEvent>>> AtomicMultiStream()
    {
        var pending = _participants.Select(s =>
        { foreach (var e in _newEvents) s = s.Apply(e); return s; }).ToArray();
        _lastBatch = await EventStreamBatch.SaveAsync(_writeStore, pending);
        return _lastBatch;
    }
}
