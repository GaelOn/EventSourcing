# EventSourcing

EventSourcing is a standalone .NET 10 library for building business aggregates whose state evolves through events. It supports immutable updates, deterministic replay, optimistic concurrency, atomic changes across related streams, and snapshots that avoid replaying old events.

The application defines what its state and events mean. The library handles event identity, stream positions, replay validation, and persistence contracts. It does not choose a database or implement business rules for you.

## Architecture: two libraries

| Package | Purpose | When to use it |
| --- | --- | --- |
| `EventSourcing` | Immutable streams, typed event handlers, supported aggregate roots, snapshots, and single/multi-stream storage contracts. Includes thread-safe in-memory stores. | New applications and business logic. |
| `EventSourcing.Compatibility` | Legacy synchronous callback aggregates and a reversible transaction coordinator. Depends on the matching core package. | Maintaining existing integrations using prepare, commit, apply, undo, and rollback callbacks. |

The dependency points from compatibility to core. Core has no dependency on compatibility, filesystem infrastructure, or an application framework.

Within core, responsibilities are separate:

- **Business root:** `EventSourcedAggregate<TState, TEvent>` exposes business operations and owned entities. It composes a stream and reads its state; there is no second state copy. This class is supported and is **not obsolete**.
- **Stream:** `EventStream<TState, TEvent>` applies events through a pure reducer, tracks versions and pending events, replays history, and saves through a storage port.
- **Event storage:** `IEventStreamStore<TEvent>` requires atomic expected-version checks and whole-batch appends. `IAtomicEventStreamStore<TEvent>` adds atomic multi-stream writes and consistent batch reads.
- **Snapshot storage:** `ISnapshotStore<TState>` stores optional checkpoints containing complete root/entity state and the corresponding committed stream version.

Only `CallbackEventSourcedAggregate` in the compatibility package is obsolete. Its coordinator supports reversible synchronous effects, but cannot turn separate durable writes into one atomic transaction. See the [compatibility contract](src/EventSourcing.Compatibility/README.md) for recovery and reentrancy requirements.

## Installation

The current prerelease is `0.1.0-alpha.5`. Build the packages locally, then reference core from your configured local feed:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet add YourApp.csproj package EventSourcing --version 0.1.0-alpha.5 --source ./artifacts/packages
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
    public static EventReducer<InventoryState, StockChanged> Reducer { get; } =
        new(Version, Reduce);

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
    inventoryId, opening, InventoryRules.Reducer);
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
    inventoryId, definition, InventoryRules.Reducer, history);
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

## Compose typed event handlers

For aggregates sensitive to several event types, implement `IEventHandler<TState, TEvent>` with pure `Do` and `Undo` methods for each transition and compose them through `EventHandlerDispatcher<TState, TEvent>`. The dispatcher supplies the stream's reducer; live updates, replay, and snapshot restoration follow the same business rules.

This alternative inventory example uses the `InventoryState` defined above and a common event base. Concrete events must preserve their runtime type, identity, and payload when cloned:

```csharp
using EventSourcing.Handlers;

public abstract class WarehouseEvent : EventBase<WarehouseEvent>
{
    public int Quantity { get; }

    protected WarehouseEvent(Guid id, int quantity, EventMetadata? metadata)
        : base(id, metadata ?? new EventMetadata(null, null))
    {
        if (quantity <= 0) throw new ArgumentOutOfRangeException(nameof(quantity));
        Quantity = quantity;
    }
}

public sealed class StockReceived(Guid id, int quantity, EventMetadata? metadata = null)
    : WarehouseEvent(id, quantity, metadata)
{
    public override WarehouseEvent Clone(EventMetadata metadata) =>
        new StockReceived(Id, Quantity, metadata);
}

public sealed class StockShipped(Guid id, int quantity, EventMetadata? metadata = null)
    : WarehouseEvent(id, quantity, metadata)
{
    public override WarehouseEvent Clone(EventMetadata metadata) =>
        new StockShipped(Id, Quantity, metadata);
}

public sealed class StockReceivedHandler : IEventHandler<InventoryState, StockReceived>
{
    public InventoryState Do(InventoryState state, StockReceived e) =>
        state with { Item = state.Item with
        { Quantity = checked(state.Item.Quantity + e.Quantity) } };

    public InventoryState Undo(InventoryState state, StockReceived e)
    {
        if (state.Item.Quantity < e.Quantity)
            throw new InvalidOperationException("Received stock is no longer available for reversal.");
        return state with { Item = state.Item with
        { Quantity = state.Item.Quantity - e.Quantity } };
    }
}

public sealed class StockShippedHandler : IEventHandler<InventoryState, StockShipped>
{
    public InventoryState Do(InventoryState state, StockShipped e)
    {
        if (e.Quantity > state.Item.Quantity)
            throw new InvalidOperationException("Insufficient stock.");
        return state with { Item = state.Item with
        { Quantity = state.Item.Quantity - e.Quantity } };
    }

    public InventoryState Undo(InventoryState state, StockShipped e) =>
        state with { Item = state.Item with
        { Quantity = checked(state.Item.Quantity + e.Quantity) } };
}
```

Register handlers in application startup code:

```csharp
var dispatcher = EventHandlerDispatcher<InventoryState, WarehouseEvent>.CreateBuilder("warehouse/v1")
    .Register<StockReceived>(new StockReceivedHandler())
    .Register<StockShipped>(new StockShippedHandler())
    .Build();

var typedStream = new EventStream<InventoryState, WarehouseEvent>(
    inventoryId, opening, dispatcher);
var typedShipmentId = Guid.Parse("33333333-3333-3333-3333-333333333333");
var updatedInventory = typedStream
    .Apply(new StockReceived(receiptId, 5))
    .Apply(new StockShipped(typedShipmentId, 2));
// Quantity 13; the original remains at 10.
```

A business root can compose this stream by deriving from `EventSourcedAggregate<InventoryState, WarehouseEvent>`. Alternatively, one business object can implement several handler interfaces and be registered once for each handled type. Methods must use their supplied state, never a captured root's current state.

For dependency injection, register instances resolved by your application's container rather than constructing them inline. A factory can express those dependencies without coupling core to a DI framework:

```csharp
public static class WarehouseHandlers
{
    public static EventHandlerDispatcher<InventoryState, WarehouseEvent> Create(
        string ruleVersion,
        IEventHandler<InventoryState, StockReceived> received,
        IEventHandler<InventoryState, StockShipped> shipped) =>
        EventHandlerDispatcher<InventoryState, WarehouseEvent>.CreateBuilder(ruleVersion)
            .Register<StockReceived>(received)
            .Register<StockShipped>(shipped)
            .Build();
}
```

Configuration may select known implementations and immutable parameters in the application's startup factory. The library does not load arbitrary type names or provide a configuration/container integration. Handler configuration and implementations form part of the recorded business `RuleVersion`; retain the corresponding factory and handlers for replay and checkpoint compatibility.

Dispatch matches the **exact runtime type**. A base-type handler is not a catch-all. Duplicate registrations throw `ArgumentException`; an unregistered type throws `UnhandledEventException` with its `EventType`. Null arguments and null handler results are rejected. Business exceptions propagate and leave the original stream unchanged.

Each `Build()` captures an immutable registration map. Later builder changes cannot alter an existing dispatcher; the builder itself is for single-threaded startup use. Handler instances are retained rather than cloned: keep them pure, reentrant, and free of mutable or disposed dependencies. No commands, external effects, or asynchronous I/O belong in these state transitions.

## Reversal, rule identity, and aggregate lifecycle

`Do` and `Undo` are pure transitions. For valid inputs, immediate reversal must satisfy `Undo(Do(state, event), event) == state`. Reversal after intervening changes is subject to domain preconditions and may fail. Replacement events must retain the previous value; their handlers should verify the current value before applying or reversing the replacement.

```csharp
var shipment = new StockShipped(typedShipmentId, 2);
var afterShipping = dispatcher.Do(opening, shipment);
var beforeShipping = dispatcher.Undo(afterShipping, shipment);
// beforeShipping equals opening; neither input was mutated.
```

Calling dispatcher `Undo` does not change a stream, erase history, or save anything. **Persisted business cancellation appends a new compensating event with a new caller-supplied ID and preserves the original event.** The business event should carry any required reversal data and causal reference. For example, cancelling a shipment can append a stock receipt, subject to the aggregate's cancellation rules.

Pass the dispatcher itself to a stream, or bind a delegate with `new EventReducer<TState, TEvent>(ruleVersion, reduce)`. Both implement `IEventReducer`, associating the mechanism with its immutable `RuleVersion`. This identity survives application, saving, full replay, and snapshot restoration. `CaptureSnapshot(schemaVersion)` and `SaveSnapshotAsync(store, schemaVersion)` derive it automatically. A bound stream rejects an explicit conflicting rule label. Rule identities remain application declarations: the library cannot prove implementation equivalence.

The aggregate exposes `Version`, `PersistedVersion`, `HistoryStartVersion`, `RuleVersion`, `PendingEvents`, and `HasPendingChanges`. Save returns a new view with no pending changes. Snapshot capture requires committed state; restored `Events` is only the suffix after `HistoryStartVersion`.

Public `Stream` access is intentional for scenario calculation, atomic batching, and composition. `Stream.Apply()` returns an independent branch and cannot mutate its containing root. Business operations should be the application's normal entry point, with invariants also enforced by its handlers/reducer. Applications needing a restricted API can expose their own façade; the generic root is not an authorization boundary.

Migration to `0.1.0-alpha.5`: handler implementations now require `Do` and `Undo`; `Handle` is a forwarding alias. Dispatcher builders require a rule version. Existing delegate/string stream overloads remain as low-level compatibility APIs; unbound delegates cannot derive snapshot identity automatically. Prefer the versioned overloads for new persisted aggregates. See the [lifecycle and reversal contracts](docs/refinement-lifecycle.md).

## Snapshot and restore

A snapshot is a checkpoint of complete business state plus its committed stream position. Save pending events first:

```csharp
var snapshots = new InMemorySnapshotStore<InventoryState>();
var root = new InventoryAggregate(saved);
await root.SaveSnapshotAsync(snapshots, schemaVersion: 1);
// Checkpoint: quantity 15 at stream version 1, including warehouse name and item.

var shipmentId = Guid.Parse("33333333-3333-3333-3333-333333333333");
var afterShipment = await root.ChangeStock(shipmentId, -2).SaveAsync(store);

var restored = await EventStream<InventoryState, StockChanged>.LoadAsync(
    inventoryId, definition, 1, InventoryRules.Reducer,
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
    InventoryRules.Reducer);

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
    checkpoint, 1, InventoryRules.Reducer,
    slices[inventoryId]);
var restoredBranch = EventStream<InventoryState, StockChanged>.Replay(
    branchId, new ReplayDefinition<InventoryState>(branch.State, InventoryRules.Version),
    InventoryRules.Reducer, slices[branchId].Events);
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

Performance measurements use the dedicated BenchmarkDotNet project, independently of correctness tests:

```sh
dotnet run --project benchmarks/EventSourcing.PerformanceTests -c Release -- --filter '*'
```

See the [performance methodology and baseline](benchmarks/EventSourcing.PerformanceTests/README.md). The older dependency-free diagnostic remains available:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet run --project benchmarks/EventSourcing.Benchmarks -c Release --no-build
```

See the [benchmark notes](benchmarks/EventSourcing.Benchmarks/README.md) for methodology and limitations.
