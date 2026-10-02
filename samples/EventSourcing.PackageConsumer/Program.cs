using EventSourcing;
using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;
using EventSourcing.Transaction;

var rules = EventHandlerDispatcher<Inventory>.CreateBuilder("consumer/v1")
    .Register<Adjusted>(new AdjustedHandler()).Register<Renamed>(new RenamedHandler()).Build();
var id = Guid.Parse("11111111-1111-1111-1111-111111111111");
var firstId = Guid.Parse("22222222-2222-2222-2222-222222222222");
var secondId = Guid.Parse("33333333-3333-3333-3333-333333333333");
var thirdId = Guid.Parse("44444444-4444-4444-4444-444444444444");
var opening = new Inventory("Original", new StockItem("BOOK", 10));
var definition = new ReplayDefinition<Inventory>(opening, rules.RuleVersion);
var store = new InMemoryEventStreamStore();
var snapshots = new InMemorySnapshotStore<Inventory>();
var root = new InventoryRoot(new EventSourcedAggregate<Inventory>(id, opening, rules));
var rename = new Renamed(secondId, "Original", "Updated");
Check(rules.Undo(rules.Do(opening, rename), rename) == opening, "Immediate reversal failed.");
root = (InventoryRoot)await root.Change(new Adjusted(firstId, 5)).Change(rename).SaveAsync(store);
await root.SaveSnapshotAsync(snapshots, 1);
root = (InventoryRoot)await root.Change(new Adjusted(thirdId, -2)).SaveAsync(store);
var loaded = await EventSourcedAggregate<Inventory>.LoadAsync(id, definition, 1, rules, store, snapshots);
var restored = new InventoryRoot(loaded);
var full = EventSourcedAggregate<Inventory>.Replay(id, definition, rules, await store.ReadAsync(id));
Check(full.State == root.State && restored.State == root.State, "Restoration differs from live state.");
Check(restored.HistoryStartVersion == 2 && restored.Version == 3 && restored.PersistedVersion == 3 &&
    !restored.HasPendingChanges && restored.PendingEvents.Count == 0 && restored.RuleVersion == "consumer/v1",
    "Aggregate lifecycle differs.");
Check(Equals(restored.Entities.Single(), restored.State.Item), "Entity state was not restored.");
try { restored.Change(new Adjusted(firstId, 1)); throw new Exception("Duplicate accepted."); }
catch (DuplicateEventException error) { Check(error.EventId == firstId && error.StreamId == id, "Duplicate context missing."); }
try { restored.CaptureSnapshot("wrong/v1", 1); throw new Exception("Rules relabelled."); }
catch (ReplayRuleVersionException error) { Check(error.StreamId == id, "Rule diagnostic missing."); }
var fourthId = Guid.Parse("55555555-5555-5555-5555-555555555555");
var tagged = new EventTransactionManager().GetEventTransactionData(new Adjusted(fourthId, 1), restored);
Check(tagged.Metadata == new EventMetadata(4, id), "Compatibility consumer cannot use the core root.");
var branchId = Guid.Parse("66666666-6666-6666-6666-666666666666");
var branch = new EventSourcedAggregate<Inventory>(branchId, opening, rules);
var savedBatch = await EventSourcedAggregateBatch.SaveAsync(store,
    new[] { loaded.Apply(new Adjusted(fourthId, -1)), branch.Apply(new Adjusted(fourthId, 1)) });
var slices = await store.ReadBatchFromAsync(new[] { new StreamReadRequest(id, 2), new StreamReadRequest(branchId, 0) });
Check(savedBatch[0].Version == 4 && slices[id].Version == 4 && slices[branchId].Version == 1, "Atomic versions differ.");
try { await loaded.Apply(new Adjusted(Guid.Parse("77777777-7777-7777-7777-777777777777"), 1)).SaveAsync(store); throw new Exception("Stale write accepted."); }
catch (StreamConcurrencyException error)
{ Check(error.ExpectedVersion == 3 && error.ActualVersion == 4 && error.StreamId == id, "Concurrency context missing."); }
Console.WriteLine("NuGet consumer passed: typed Do/Undo, entities, bound rules, replay, snapshots, duplicates, atomic reads/writes, diagnostics, compatibility dependency.");

static void Check(bool valid, string reason) { if (!valid) throw new InvalidOperationException(reason); }
public sealed record StockItem(string Sku, int Quantity) : IEntity;
public sealed record Inventory(string Name, StockItem Item);
public sealed class Adjusted(Guid id, int delta, EventMetadata? metadata = null) : EventBase<Adjusted>(id, metadata ?? new(null, null))
{
    public int Delta { get; } = delta;
    public override Adjusted Clone(EventMetadata metadata) => new Adjusted(Id, Delta, metadata);
}
public sealed class Renamed(Guid id, string previous, string next, EventMetadata? metadata = null) : EventBase(id, metadata ?? new(null, null))
{
    public string Previous { get; } = previous;
    public string Next { get; } = next;
    public override IEvent Clone(EventMetadata metadata) => new Renamed(Id, Previous, Next, metadata);
}
public sealed class AdjustedHandler : IEventHandler<Inventory, Adjusted>
{
    public Inventory Do(Inventory state, Adjusted e) => state with { Item = state.Item with { Quantity = checked(state.Item.Quantity + e.Delta) } };
    public Inventory Undo(Inventory state, Adjusted e) => state with { Item = state.Item with { Quantity = checked(state.Item.Quantity - e.Delta) } };
}
public sealed class RenamedHandler : IEventHandler<Inventory, Renamed>
{
    public Inventory Do(Inventory state, Renamed e) => state.Name == e.Previous
        ? state with { Name = e.Next } : throw new InvalidOperationException("Previous name does not match.");
    public Inventory Undo(Inventory state, Renamed e) => state.Name == e.Next
        ? state with { Name = e.Previous } : throw new InvalidOperationException("Name changed since the event.");
}
public sealed class InventoryRoot(EventSourcedAggregate<Inventory> stream) : EventSourcedAggregate<Inventory>(stream)
{
    public override IEnumerable<IEntity> Entities => new IEntity[] { State.Item };
    public InventoryRoot Change(IEvent e) => (InventoryRoot)Apply(e);
}
