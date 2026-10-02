using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class RefinementTests
{
    [Fact]
    public void A_clone_changing_type_is_rejected_before_reduction_and_preserves_pending_work()
    {
        var calls = 0;
        var stream = new EventSourcedAggregate<Value>(StreamTests.StreamId, new Value(10),
            new EventReducer<Value>("value/v1", (s, e) => { calls++; return new(s.Amount + 1); }));
        var pending = stream.Apply(new GoodChange(StreamTests.FirstId));
        var error = Assert.Throws<EventCloneException>(() => pending.Apply(new BadChange(StreamTests.SecondId)));
        Assert.Equal(StreamTests.SecondId, error.EventId);
        Assert.Equal(typeof(BadChange), error.OriginalType);
        Assert.Equal(typeof(GoodChange), error.ClonedType);
        Assert.Equal(1, calls);
        Assert.Equal(new Value(11), pending.State);
        Assert.Equal(1, pending.Version);
        Assert.Equal(0, pending.PersistedVersion);
        Assert.Equal(StreamTests.FirstId, Assert.Single(pending.PendingEvents).Id);
    }

    [Fact]
    public void Immediate_reversal_restores_previous_values_without_mutating_inputs()
    {
        var dispatcher = EventHandlerDispatcher<Value>.CreateBuilder("value/v1")
            .Register<Replacement>(new ReplacementHandler()).Build();
        var original = new Value(10);
        var e = new Replacement(StreamTests.FirstId, 10, 25);
        var changed = dispatcher.Do(original, e);
        Assert.Equal(original, dispatcher.Undo(changed, e));
        Assert.Equal(new Value(10), original);
        Assert.Equal(new Value(25), changed);
        Assert.Throws<InvalidOperationException>(() => dispatcher.Do(new Value(9), e));
        Assert.Throws<InvalidOperationException>(() => dispatcher.Undo(new Value(26), e));
    }

    [Fact]
    public async Task Persisted_cancellation_appends_compensation_and_retains_the_original_event()
    {
        var dispatcher = EventHandlerDispatcher<Value>.CreateBuilder("value/v1")
            .Register<Replacement>(new ReplacementHandler()).Build();
        var store = new InMemoryEventStreamStore();
        var stream = new EventSourcedAggregate<Value>(StreamTests.StreamId, new Value(10), dispatcher);
        var saved = await stream.HandleAsync(new Replacement(StreamTests.FirstId, 10, 25), store);
        var cancelled = await saved.HandleAsync(new Replacement(StreamTests.SecondId, 25, 10), store);
        var history = await store.ReadAsync(stream.Id);
        Assert.Equal(new Value(10), cancelled.State);
        Assert.Equal(2, cancelled.Version);
        Assert.Equal(new[] { StreamTests.FirstId, StreamTests.SecondId }, history.Select(e => e.Id));
        Assert.Equal(cancelled.State, EventSourcedAggregate<Value>.Replay(stream.Id,
            new ReplayDefinition<Value>(new Value(10), "value/v1"), dispatcher, history).State);
    }

    [Fact]
    public void Snapshots_derive_rules_from_the_calculation_and_cannot_be_relabelled()
    {
        var reducer = new EventReducer<Value>("value/v1", (s, e) => s);
        var stream = new EventSourcedAggregate<Value>(StreamTests.StreamId, new Value(10), reducer);
        Assert.Equal("value/v1", stream.RuleVersion);
        Assert.Equal("value/v1", stream.CaptureSnapshot(1).RuleVersion);
        Assert.Throws<ReplayRuleVersionException>(() => stream.CaptureSnapshot("value/v2", 1));
        Assert.Throws<ReplayRuleVersionException>(() => EventSourcedAggregate<Value>.Replay(stream.Id,
            new ReplayDefinition<Value>(new Value(10), "value/v2"), reducer, []));
    }

    [Fact]
    public async Task Aggregate_lifecycle_exposes_snapshot_boundary_and_pending_changes()
    {
        var reducer = new EventReducer<Value>("value/v1", (s, e) => new(s.Amount + 1));
        var store = new InMemoryEventStreamStore();
        var saved = await new EventSourcedAggregate<Value>(StreamTests.StreamId, new Value(10), reducer)
            .HandleAsync(new GoodChange(StreamTests.FirstId), store);
        var snapshot = saved.CaptureSnapshot(1);
        var loaded = EventSourcedAggregate<Value>.Replay(snapshot, reducer, await store.ReadFromAsync(saved.Id, 1));
        var root = new Root(loaded.Apply(new GoodChange(StreamTests.SecondId)));
        Assert.Equal(1, root.HistoryStartVersion);
        Assert.Equal(1, root.PersistedVersion);
        Assert.Equal(2, root.Version);
        Assert.True(root.HasPendingChanges);
        Assert.Single(root.PendingEvents);
        Assert.Equal("value/v1", root.RuleVersion);
        Assert.Throws<InvalidOperationException>(() => root.CaptureSnapshot(1));
    }

    [Fact]
    public void Diagnostics_expose_values_without_message_parsing()
    {
        var concurrency = new StreamConcurrencyException(1, 2, StreamTests.StreamId);
        Assert.Equal(1, concurrency.ExpectedVersion);
        Assert.Equal(2, concurrency.ActualVersion);
        Assert.Equal(StreamTests.StreamId, concurrency.StreamId);
        var duplicate = new DuplicateEventException(StreamTests.FirstId, StreamTests.StreamId);
        Assert.Equal(StreamTests.FirstId, duplicate.EventId);
        Assert.Equal(StreamTests.StreamId, duplicate.StreamId);
    }

    private sealed record Value(int Amount);
    private abstract class Change(Guid id, EventMetadata? metadata = null) : EventBase<Change>(id, metadata ?? new(null, null));
    private sealed class GoodChange(Guid id, EventMetadata? metadata = null) : Change(id, metadata)
    {
        public override Change Clone(EventMetadata metadata) => new GoodChange(Id, metadata);
    }
    private sealed class BadChange(Guid id) : Change(id)
    {
        public override Change Clone(EventMetadata metadata) => new GoodChange(Id, metadata);
    }
    private sealed class Replacement(Guid id, int previous, int next, EventMetadata? metadata = null) : Change(id, metadata)
    {
        public int Previous { get; } = previous;
        public int Next { get; } = next;
        public override Change Clone(EventMetadata metadata) => new Replacement(Id, Previous, Next, metadata);
    }
    private sealed class ReplacementHandler : IEventHandler<Value, Replacement>
    {
        public Value Do(Value state, Replacement e) => state.Amount == e.Previous
            ? new(e.Next) : throw new InvalidOperationException("Previous value no longer matches.");
        public Value Undo(Value state, Replacement e) => state.Amount == e.Next
            ? new(e.Previous) : throw new InvalidOperationException("Reversal requires the immediate resulting value.");
    }
    private sealed class Root(EventSourcedAggregate<Value> stream) : EventSourcedAggregate<Value>(stream)
    {
        public override IEnumerable<IEntity> Entities => Array.Empty<IEntity>();
    }
}
