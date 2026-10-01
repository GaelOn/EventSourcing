# EventSourcing

EventSourcing is a standalone .NET 10 library for building business aggregates whose state evolves through events. It supports immutable updates, deterministic replay, optimistic concurrency, atomic changes across related streams, and snapshots that avoid replaying old events.

The application defines what its state and events mean. The library handles event identity, stream positions, replay validation, and persistence contracts. It does not choose a database or implement business rules for you.

## Architecture: two libraries

| Package | Purpose | When to use it |
| --- | --- | --- |
| `EventSourcing` | Immutable streams, supported aggregate roots, snapshots, and single/multi-stream storage contracts. Includes thread-safe in-memory stores. | New applications and business logic. |
| `EventSourcing.Compatibility` | Legacy synchronous callback aggregates and a reversible transaction coordinator. Depends on the matching core package. | Maintaining existing integrations using prepare, commit, apply, undo, and rollback callbacks. |

The dependency points from compatibility to core. Core has no dependency on compatibility, filesystem infrastructure, or an application framework.

Within core, responsibilities are separate:

- **Business root:** `EventSourcedAggregate<TState, TEvent>` exposes business operations and owned entities. It composes a stream and reads its state; there is no second state copy. This class is supported and is **not obsolete**.
- **Stream:** `EventStream<TState, TEvent>` applies events through a pure reducer, tracks versions and pending events, replays history, and saves through a storage port.
- **Event storage:** `IEventStreamStore<TEvent>` requires atomic expected-version checks and whole-batch appends. `IAtomicEventStreamStore<TEvent>` adds atomic multi-stream writes and consistent batch reads.
- **Snapshot storage:** `ISnapshotStore<TState>` stores optional checkpoints containing complete root/entity state and the corresponding committed stream version.

Only `CallbackEventSourcedAggregate` in the compatibility package is obsolete. Its coordinator supports reversible synchronous effects, but cannot turn separate durable writes into one atomic transaction. See the [compatibility contract](src/EventSourcing.Compatibility/README.md) for recovery and reentrancy requirements.

## Installation

The current prerelease is `0.1.0-alpha.3`. Build the packages locally, then reference core from your configured local feed:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet add YourApp.csproj package EventSourcing --version 0.1.0-alpha.3 --source ./artifacts/packages
```

Add `EventSourcing.Compatibility` only if you need the legacy callback API. Packages and portable symbols are generated in `artifacts/packages/`; no package-feed publication is configured.

## Define immutable state, events, and rules

The following examples use a small inventory aggregate. All snippets share these types and namespace imports:

```csharp
using EventSourcing;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

public sealed record StockItem(string Sku, int Quantity) : IEntity;
public sealed record InventoryState(string Name, StockItem Item);

public sealed class StockChanged : EventBase<StockChanged>
{
    public int Delta { get; }

    public StockChanged(Guid id, int delta)
        : this(id, delta, new EventMetadata(null, null)) { }

    private StockChanged(Guid id, int delta, EventMetadata metadata)
        : base(id, metadata) => Delta = delta;

    public override StockChanged Clone(EventMetadata metadata) =>
        new(Id, Delta, metadata);
}

public static class InventoryRules
{
    public const string Version = "inventory/v1";

    public static InventoryState Reduce(InventoryState state, StockChanged e)
    {
        var quantity = checked(state.Item.Quantity + e.Delta);
        if (quantity < 0)
            throw new InvalidOperationException("Stock cannot become negative.");

        return state with { Item = state.Item with { Quantity = quantity } };
    }
}
```

The caller supplies a nonempty event GUID. `Clone` preserves that identity and payload while replacing metadata. Acceptance assigns a stream ID and a one-based version to a copy. Event IDs and stream versions are different concepts; duplicates are rejected per stream.

State and events must be deeply immutable. Reducers must be pure and reentrant: no mutation, I/O, generated IDs, or ambient time. Use the same rules for live calculation and replay.

## Apply and save events

```csharp
var inventoryId = Guid.Parse("11111111-1111-1111-1111-111111111111");
var receiptId = Guid.Parse("22222222-2222-2222-2222-222222222222");
var opening = new InventoryState("Main warehouse", new StockItem("BOOK", 10));
var definition = new ReplayDefinition<InventoryState>(opening, InventoryRules.Version);
var store = new InMemoryEventStreamStore<StockChanged>();

var original = new EventStream<InventoryState, StockChanged>(
    inventoryId, opening, InventoryRules.Reduce);
var changed = original.Apply(new StockChanged(receiptId, +5));
// original: quantity 10, version 0.
// changed: quantity 15, version 1, persisted version 0, one pending event.

var saved = await changed.SaveAsync(store);
// saved: quantity 15, version 1, persisted version 1, no pending events.
```

`Apply` returns a new stream. Reduction or storage failure leaves the input stream and its pending history intact. Always continue with the returned saved stream. `HandleAsync(event, store)` combines application and saving when you want one operation.

Saving checks `PersistedVersion` against storage. A competing writer causes `StreamConcurrencyException`; reload and reconcile before retrying. The included memory store is thread-safe but non-durable: share one instance per ledger.

## Replay with the original opening inputs

```csharp
var history = await store.ReadAsync(inventoryId);
var replayed = EventStream<InventoryState, StockChanged>.Replay(
    inventoryId, definition, InventoryRules.Version, InventoryRules.Reduce, history);
// quantity 15; no pending events and no storage or side-effect callbacks during replay.
```

Persist the opening definition alongside the stream and retain the reducer implementing its rule version. The event store stores events only; it does not store `ReplayDefinition`. The guarded overload rejects a supplied rule-version mismatch before reduction. It cannot verify the implementation of a delegate or choose the correct opening inputs for you.

Replay validates stream identity, contiguous positions, and unique event IDs. Stream membership follows version order; a business effective date in an event does not reorder that history.

## Expose a business aggregate root

A root gives the stream business meaning and exposes its entities. Its methods wrap updated streams in new roots:

```csharp
public sealed class InventoryAggregate
    : EventSourcedAggregate<InventoryState, StockChanged>
{
    public InventoryAggregate(EventStream<InventoryState, StockChanged> stream)
        : base(stream) { }

    public string Name => State.Name;
    public StockItem Item => State.Item;
    public override IEnumerable<IEntity> Entities => new IEntity[] { Item };

    public InventoryAggregate ChangeStock(Guid eventId, int delta) =>
        new(Stream.Apply(new StockChanged(eventId, delta)));

    public async Task<InventoryAggregate> SaveAsync(IEventStreamStore<StockChanged> store) =>
        new(await Stream.SaveAsync(store));
}

// In your application method:
// var root = new InventoryAggregate(saved);
// var updated = root.ChangeStock(callerSuppliedEventId, -2);
// var savedRoot = await updated.SaveAsync(store);
```

Business invariants belong in the root and reducer. `State` must contain all immutable information needed to reconstruct the root and its owned entities. Services, stores, reducers, and callbacks belong outside that state.

## Snapshot and restore

A snapshot is a checkpoint of complete business state plus its committed stream position. Save pending events first:

```csharp
var snapshots = new InMemorySnapshotStore<InventoryState>();
var root = new InventoryAggregate(saved);
await root.SaveSnapshotAsync(snapshots, InventoryRules.Version, schemaVersion: 1);
// Checkpoint: quantity 15 at stream version 1, including warehouse name and item.

var shipmentId = Guid.Parse("33333333-3333-3333-3333-333333333333");
var afterShipment = await root.ChangeStock(shipmentId, -2).SaveAsync(store);

var restored = await EventStream<InventoryState, StockChanged>.LoadAsync(
    inventoryId, definition, InventoryRules.Version, 1, InventoryRules.Reduce,
    store, snapshots);
var restoredRoot = new InventoryAggregate(restored);
// quantity 13, version 2, HistoryStartVersion 1; only the shipment was reduced.
```

Checkpoint compatibility uses aggregate identity, rule version, and state schema version. A missing compatible checkpoint falls back to full replay from the opening definition. Corruption and storage errors propagate; a checkpoint ahead of the ledger is rejected.

Capture rejects pending changes. Snapshot saving is separate from event committing: checkpoint-write failure does not undo committed events. Older or equal checkpoints cannot replace a newer checkpoint for the same compatibility key.

After restoration, `Events` contains only the retained suffix after `HistoryStartVersion`. Use `Version` for the complete stream position, never `Events.Count`. Earlier event IDs are still loaded to reject duplicates immediately, so snapshot restoration avoids old payload reduction but retains an identity-index cost proportional to prior history. Full history and opening definitions remain in storage; snapshots do not delete events.

## Save related streams atomically

A stock transfer must update both warehouses together. The same caller-supplied ID may appear in separate streams to identify the shared operation:

```csharp
var branchId = Guid.Parse("44444444-4444-4444-4444-444444444444");
var transferId = Guid.Parse("55555555-5555-5555-5555-555555555555");
var branch = new EventStream<InventoryState, StockChanged>(
    branchId, new InventoryState("Branch", new StockItem("BOOK", 0)),
    InventoryRules.Reduce);

var participants = await EventStreamBatch.SaveAsync(store, new[]
{
    afterShipment.Stream.Apply(new StockChanged(transferId, -3)),
    branch.Apply(new StockChanged(transferId, +3))
});
// Main: 10, branch: 3; total stock remains 13. Both writes succeed or neither does.

var histories = await store.ReadBatchAsync(new[] { inventoryId, branchId });
// Both histories come from one consistent ledger view.
```

Every participant's expected version is checked, including unchanged streams included as read dependencies. Failure preserves all input streams and pending events. The application must include every affected aggregate and validate business reconciliation; storage cannot infer missing participants.

For consistent snapshot restoration of several roots, choose their compatible checkpoints, then read all suffixes with **one** batch request:

```csharp
// Main has the earlier checkpoint; branch will replay from its opening state.
var checkpoint = await snapshots.ReadAsync(inventoryId, InventoryRules.Version, 1)
    ?? throw new InvalidOperationException("Expected the example checkpoint.");
var slices = await store.ReadBatchFromAsync(new[]
{
    new StreamReadRequest(inventoryId, checkpoint.StreamVersion),
    new StreamReadRequest(branchId, 0)
});
var restoredMain = EventStream<InventoryState, StockChanged>.Replay(
    checkpoint, InventoryRules.Version, 1, InventoryRules.Reduce,
    slices[inventoryId]);
var restoredBranch = EventStream<InventoryState, StockChanged>.Replay(
    branchId, new ReplayDefinition<InventoryState>(branch.State, InventoryRules.Version),
    InventoryRules.Version, InventoryRules.Reduce,
    slices[branchId].Events);
```

Independent `LoadAsync` calls can observe different commits. Batch reads bring participants to one consistent view even when their checkpoints have different versions. Atomicity covers one store's transaction boundary; cross-database transactions are not provided.

## Persistence and compatibility limits

A durable adapter must enforce expected versions, duplicate validation, and whole-batch atomicity inside one storage transaction. Cancellation before commit leaves storage unchanged; after commit the adapter must report success. The library provides contracts and in-memory implementations, not a production database adapter, automatic conflict merging, checkpoint scheduling, or retries.

The JSON ledger and snapshot adapters under `src/Test` are integration-test fixtures. They use atomic document replacement and still read complete files; they demonstrate correctness rather than database range-read performance. The solution is independent of Family Horizon.

For detailed contracts and breaking changes in `0.1.0-alpha.3`, see the [core documentation](src/EventSourcing/README.md) and [compatibility documentation](src/EventSourcing.Compatibility/README.md).

## Build, test, and CI

From this repository root:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx --no-restore
dotnet test EventSourcing.slnx --no-build
```

GitHub Actions builds Release with warnings as errors, runs tests, and uploads NuGet packages, portable symbols, and TRX results. Package artifacts are retained for 30 days and test results for 14 days. Download them from the workflow run; no feed publication is configured.

For the dependency-free performance diagnostic:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet run --project benchmarks/EventSourcing.Benchmarks -c Release --no-build
```

See the [benchmark notes](benchmarks/EventSourcing.Benchmarks/README.md) for methodology and limitations.
