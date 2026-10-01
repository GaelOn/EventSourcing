using EventSourcing;
using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

var dispatcher = EventHandlerDispatcher<Value, Change>.CreateBuilder("consumer/v1")
    .Register<Change>(new ChangeHandler()).Build();
var opening = new Value(10);
var e = new Change(Guid.NewGuid(), 10, 15);
Require(dispatcher.Undo(dispatcher.Do(opening, e), e) == opening);
var store = new InMemoryEventStreamStore<Change>();
var snapshots = new InMemorySnapshotStore<Value>();
var original = new Root(new EventStream<Value, Change>(Guid.NewGuid(), opening, dispatcher));
var saved = new Root(await original.Stream.HandleAsync(e, store));
await saved.SaveSnapshotAsync(snapshots, 1);
var loaded = await EventStream<Value, Change>.LoadAsync(saved.Id,
    new ReplayDefinition<Value>(opening, dispatcher.RuleVersion), 1, dispatcher, store, snapshots);
Require(loaded.State == saved.State && loaded.Version == 1 && loaded.HistoryStartVersion == 1);
Require(saved.RuleVersion == "consumer/v1" && !saved.HasPendingChanges && saved.PendingEvents.Count == 0);
try { loaded.Apply(new Change(e.Id, 15, 20)); throw new Exception("Duplicate accepted."); }
catch (DuplicateEventException error) { Require(error.EventId == e.Id && error.StreamId == loaded.Id); }
var next = loaded.Apply(new Change(Guid.NewGuid(), 15, 20));
var other = new EventStream<Value, Change>(Guid.NewGuid(), opening,
    new EventReducer<Value, Change>("consumer/v1", dispatcher.Do)).Apply(new Change(Guid.NewGuid(), 10, 12));
var batch = await EventStreamBatch.SaveAsync(store, new[] { next, other });
Require(batch.All(s => s.PendingEvents.Count == 0));
var slices = await store.ReadBatchFromAsync(new[] { new StreamReadRequest(next.Id, 1), new StreamReadRequest(other.Id, 0) });
Require(EventStream<Value, Change>.Replay(saved.CaptureSnapshot(1), 1, dispatcher, slices[next.Id]).State == new Value(20));
try { await next.SaveAsync(store); throw new Exception("Stale write accepted."); }
catch (StreamConcurrencyException error) { Require(error.ExpectedVersion == 1 && error.ActualVersion == 2 && error.StreamId == next.Id); }
Console.WriteLine("Package consumer passed: handlers, reversal, aggregates, snapshots, duplicate diagnostics and atomic persistence.");
static void Require(bool condition) { if (!condition) throw new Exception("Consumer check failed."); }
sealed record Value(decimal Amount);
sealed class Change(Guid id, decimal previous, decimal next, EventMetadata? metadata = null)
    : EventBase<Change>(id, metadata ?? new(null, null))
{
    public decimal Previous { get; } = previous;
    public decimal Next { get; } = next;
    public override Change Clone(EventMetadata metadata) => new(Id, Previous, Next, metadata);
}
sealed class ChangeHandler : IEventHandler<Value, Change>
{
    public Value Do(Value state, Change e) => state.Amount == e.Previous ? new(e.Next) : throw new InvalidOperationException("Previous value mismatch.");
    public Value Undo(Value state, Change e) => state.Amount == e.Next ? new(e.Previous) : throw new InvalidOperationException("Immediate result required.");
}
sealed class Root(EventStream<Value, Change> stream) : EventSourcedAggregate<Value, Change>(stream)
{
    public override IEnumerable<IEntity> Entities => Array.Empty<IEntity>();
}
