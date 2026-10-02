using EventSourcing.ObjectsAggregation;
using System.Collections.Immutable;
using System.Text.Json;
using System.Buffers.Binary;
using System.Security.Cryptography;
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
    public Balance Do(Balance s, AmountEvent e) => new(s.Amount + ((AmountEvent)e).Amount);
    public Balance Undo(Balance s, AmountEvent e) => new(s.Amount - ((AmountEvent)e).Amount);
}

public sealed class OtherAmountEvent(Guid id, decimal amount, EventMetadata? metadata = null)
    : EventBase(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public override IEvent Clone(EventMetadata metadata) => new OtherAmountEvent(Id, Amount, metadata);
}
public sealed class OtherAmountHandler : IEventHandler<Balance, OtherAmountEvent>
{
    public Balance Do(Balance s, OtherAmountEvent e) => new(s.Amount + e.Amount);
    public Balance Undo(Balance s, OtherAmountEvent e) => new(s.Amount - e.Amount);
}

[MemoryDiagnoser]
public class StreamBenchmarks
{
    public IEnumerable<int> HistoryCounts => Baseline || BatchProfile ? new[] { 10000 } : new[] { 100, 10000 };
    public IEnumerable<int> SuffixLengths => BatchProfile ? new[] { 100 } : Baseline ? new[] { 1 } : new[] { 1, 100 };
    public IEnumerable<int> ParticipantCounts => Baseline || BatchProfile ? new[] { 2 } : new[] { 2, 8 };
    private static bool Baseline => Environment.GetEnvironmentVariable("EVENTSOURCING_BENCHMARK_PROFILE") == "baseline";
    private static bool BatchProfile => Environment.GetEnvironmentVariable("EVENTSOURCING_BENCHMARK_PROFILE") == "batch";
    public IEnumerable<int> SnapshotPositions => BatchProfile ? new[] { 90 } : new[] { 10, 90 };
    [ParamsSource(nameof(HistoryCounts))] public int HistoricalEventCount { get; set; }
    // Percent of historical events covered by the checkpoint: older versus recent.
    [ParamsSource(nameof(SnapshotPositions))] public int SnapshotPositionPercent { get; set; }
    [ParamsSource(nameof(SuffixLengths))] public int SuffixLength { get; set; }
    [ParamsSource(nameof(ParticipantCounts))] public int ParticipantCount { get; set; }
    private readonly Balance _dispatchOpening = new(0);
    private ReplayDefinition<Balance> _definition = null!;
    private EventSourcedAggregate<Balance> _stream = null!;
    private EventReducer<Balance> _rules = null!;
    private EventHandlerDispatcher<Balance> _dispatcher = null!;
    private InMemoryEventStreamStore _store = null!;
    private InMemoryEventStreamStore _writeStore = null!;
    private EventSourcedAggregate<Balance>[] _participants = null!;
    private IReadOnlyList<IEvent> _history = null!;
    private AggregateSnapshot<Balance> _snapshot = null!;
    private StreamSlice _slice = null!;
    private AmountEvent[] _newEvents = null!;
    private Guid[] _prefixIds = null!;
    private EventSourcedAggregate<Balance>? _lastApplied;
    private IEvent[] _heterogeneousHistory = null!;
    private IEvent[] _homogeneousArrayHistory = null!;
    private IEvent[] _heterogeneousNewEvents = null!;
    private EventSourcedAggregate<Balance> _handlerStream = null!;
    private EventSourcedAggregate<Balance> _heterogeneousHandlerStream = null!;
    private OtherAmountEvent _otherDispatchEvent = null!;
    private IReadOnlyList<EventSourcedAggregate<Balance>>? _lastBatch;

    // Stable pseudorandom-looking IDs give each process the same identity-index corpus.
    // ID generation is setup only, never part of a timed transition.
    private static Guid FixtureId(int scope, int position)
    {
        Span<byte> input = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(input, scope);
        BinaryPrimitives.WriteInt32LittleEndian(input[4..], position);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input, hash);
        return new Guid(hash[..16]);
    }

    [GlobalSetup]
    public void Setup()
    {
        _rules = new("balance/v1", (s, e) => new(s.Amount + ((AmountEvent)e).Amount));
        _dispatcher = EventHandlerDispatcher<Balance>.CreateBuilder("balance/v1")
            .Register<AmountEvent>(new AmountHandler()).Register<OtherAmountEvent>(new OtherAmountHandler()).Build();
        _definition = new(new(0), _rules.RuleVersion);
        _store = new();
        var id = FixtureId(1, 0);
        var stream = new EventSourcedAggregate<Balance>(id, new(0), _rules);
        var cursor = HistoricalEventCount * SnapshotPositionPercent / 100;
        for (var i = 0; i < HistoricalEventCount + SuffixLength; i++)
        {
            stream = stream.Apply(new AmountEvent(FixtureId(2, i + 1), 1));
            if (i + 1 == cursor)
            {
                stream = stream.SaveAsync(_store).GetAwaiter().GetResult();
                _snapshot = stream.CaptureSnapshot(1);
            }
        }
        _stream = stream.SaveAsync(_store).GetAwaiter().GetResult();
        _history = _store.ReadAsync(id).GetAwaiter().GetResult();
        _homogeneousArrayHistory = _history.ToArray();
        _heterogeneousHistory = _history.Select((e, i) => i % 2 == 0 ? e : new OtherAmountEvent(e.Id, 1m, e.Metadata)).ToArray();
        _handlerStream = EventSourcedAggregate<Balance>.Replay(id, _definition, _dispatcher, _homogeneousArrayHistory);
        _heterogeneousHandlerStream = EventSourcedAggregate<Balance>.Replay(id, _definition, _dispatcher, _heterogeneousHistory);
        _otherDispatchEvent = new OtherAmountEvent(FixtureId(4, 0), 1m);
        _slice = _store.ReadFromAsync(id, cursor).GetAwaiter().GetResult();
        _prefixIds = _history.Take(cursor).Select(e => e.Id).ToArray();
        _newEvents = Enumerable.Range(0, SuffixLength).Select(i => new AmountEvent(FixtureId(3, i + 1), 1)).ToArray();
        _heterogeneousNewEvents = _newEvents.Select((e, i) => i % 2 != 0 ? (IEvent)e : new OtherAmountEvent(e.Id, 1m)).ToArray();
        _participants = Enumerable.Range(0, ParticipantCount).Select(index =>
        {
            var participantId = FixtureId(5, index + 1);
            var history = _history.Select(e => new AmountEvent(e.Id, ((AmountEvent)e).Amount,
                new EventMetadata(e.Metadata.Version, participantId))).ToArray();
            return EventSourcedAggregate<Balance>.Replay(participantId,
                new ReplayDefinition<Balance>(new(0), _rules.RuleVersion), _rules, history);
        }).ToArray();
        Check(FullReplay(), HistoricalEventCount + SuffixLength);
        Check(SnapshotRestore(), HistoricalEventCount + SuffixLength);
        Check(SnapshotLoadIncludingHistoricalIds().GetAwaiter().GetResult(), HistoricalEventCount + SuffixLength);
        Check(Apply(), HistoricalEventCount + 2 * SuffixLength);
        Check(HomogeneousHandlerApply(), HistoricalEventCount + 2 * SuffixLength);
        Check(HeterogeneousApply(), HistoricalEventCount + 2 * SuffixLength);
        Check(HeterogeneousReplay(), HistoricalEventCount + SuffixLength);
        Check(HomogeneousHandlerReplay(), HistoricalEventCount + SuffixLength);
        if (Append().Version != _stream.Version + SuffixLength) throw new InvalidOperationException("Append mismatch.");
        if (HeterogeneousDispatch() != new Balance(1)) throw new InvalidOperationException("Heterogeneous dispatch mismatch.");
        if (JsonSerializer.Deserialize<AggregateSnapshot<Balance>>(SnapshotSerialization())!.State != _snapshot.State)
            throw new InvalidOperationException("Snapshot serialization mismatch.");
        if (BuildHistoricalIdentityIndex().Count != cursor) throw new InvalidOperationException("Prefix ID index mismatch.");
        if (LoadHistoricalIds().PriorEventIds.Count != cursor) throw new InvalidOperationException("Slice identity mismatch.");
        if (Dispatch() != new Balance(1)) throw new InvalidOperationException("Dispatch mismatch.");
        ResetWrites();
        Check(ApplyAndSave().GetAwaiter().GetResult(), HistoricalEventCount + 2 * SuffixLength);
        ResetWrites();
        Check(SaveEachEvent().GetAwaiter().GetResult(), HistoricalEventCount + 2 * SuffixLength);
        ResetWrites();
        AtomicMultiStream().GetAwaiter().GetResult();
        Validate();
    }

    [IterationSetup(Targets = new[] { nameof(ApplyAndSave), nameof(SaveEachEvent), nameof(AtomicMultiStream) })]
    public void ResetWrites()
    {
        _writeStore = new();
        _writeStore.AppendAsync(_stream.Id, 0, _history).GetAwaiter().GetResult();
        _writeStore.AppendBatchAsync(_participants.Select(s =>
            new StreamAppend(s.Id, 0, s.Events)).ToArray()).GetAwaiter().GetResult();
        _lastApplied = null; _lastBatch = null;
    }
    [IterationCleanup(Targets = new[] { nameof(ApplyAndSave), nameof(SaveEachEvent), nameof(AtomicMultiStream) })]
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
    private static void Check(EventSourcedAggregate<Balance> s, int count)
    {
        if (s.State.Amount != count || s.Version != count || s.Events.Count != s.Version - s.HistoryStartVersion ||
            s.PendingEvents.Count != s.Version - s.PersistedVersion)
            throw new InvalidOperationException("Workload mismatch.");
    }

    [Benchmark] public EventSourcedAggregate<Balance> Apply()
    {
        var s = _stream;
        foreach (var e in _newEvents) s = s.Apply(e);
        return s;
    }
    [Benchmark] public async Task<EventSourcedAggregate<Balance>> ApplyAndSave()
    { _lastApplied = await Apply().SaveAsync(_writeStore); return _lastApplied; }
    [Benchmark] public EventSourcedAggregate<Balance> FullReplay() => EventSourcedAggregate<Balance>.Replay(
        _stream.Id, _definition, _rules, _history);
    [Benchmark] public EventSourcedAggregate<Balance> SnapshotRestore() =>
        EventSourcedAggregate<Balance>.Replay(_snapshot, 1, _rules, _slice);
    [Benchmark] public async Task<EventSourcedAggregate<Balance>> SnapshotLoadIncludingHistoricalIds() =>
        EventSourcedAggregate<Balance>.Replay(_snapshot, 1, _rules, await _store.ReadFromAsync(_stream.Id, _snapshot.StreamVersion));
    [Benchmark] public ImmutableHashSet<Guid> BuildHistoricalIdentityIndex() => _prefixIds.ToImmutableHashSet();
    [Benchmark] public StreamSlice LoadHistoricalIds() => new(_stream.Id,
        _snapshot.StreamVersion, _stream.Version, _slice.Events, _prefixIds);
    [Benchmark] public Balance Dispatch() => _dispatcher.Do(_dispatchOpening, _newEvents[0]);
    [Benchmark] public EventStream Append()
    {
        var stream = _stream.Stream;
        foreach (var e in _newEvents) stream = stream.Append(e);
        return stream;
    }
    [Benchmark] public EventSourcedAggregate<Balance> HomogeneousHandlerApply() => _handlerStream.ApplyBatch(_newEvents);
    [Benchmark] public EventSourcedAggregate<Balance> HeterogeneousApply() => _heterogeneousHandlerStream.ApplyBatch(_heterogeneousNewEvents);
    [Benchmark] public EventSourcedAggregate<Balance> HomogeneousHandlerReplay() =>
        EventSourcedAggregate<Balance>.Replay(_stream.Id, _definition, _dispatcher, _homogeneousArrayHistory);
    [Benchmark] public EventSourcedAggregate<Balance> HeterogeneousReplay() =>
        EventSourcedAggregate<Balance>.Replay(_stream.Id, _definition, _dispatcher, _heterogeneousHistory);
    [Benchmark] public Balance HeterogeneousDispatch() => _dispatcher.Do(_dispatchOpening, _otherDispatchEvent);
    [Benchmark] public byte[] SnapshotSerialization() => JsonSerializer.SerializeToUtf8Bytes(_snapshot);
    [Benchmark] public async Task<EventSourcedAggregate<Balance>> SaveEachEvent()
    {
        var root = _stream;
        foreach (var e in _newEvents) root = await root.HandleAsync(e, _writeStore);
        _lastApplied = root;
        return root;
    }
    [Benchmark] public async Task<IReadOnlyList<EventSourcedAggregate<Balance>>> AtomicMultiStream()
    {
        var pending = _participants.Select(s =>
        { foreach (var e in _newEvents) s = s.Apply(e); return s; }).ToArray();
        _lastBatch = await EventSourcedAggregateBatch.SaveAsync(_writeStore, pending);
        return _lastBatch;
    }
}
