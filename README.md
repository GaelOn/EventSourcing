# EventSourcing

Licensed under the [MIT License](LICENSE). Copyright (c) 2026 Gaël Dupire.

A standalone .NET 10 library for immutable business aggregates, deterministic replay, optimistic
concurrency, atomic multi-aggregate persistence, and complete state snapshots. Version **0.2.0**
is a breaking API refactor; see the [migration guide](docs/migration-0.2.0.md).

## Responsibilities

| Component | Responsibility |
| --- | --- |
| `EventSourcedAggregate<TState>` | Owns the single immutable business state, bound transition rules, and matching stream; applies events, replays, saves, captures snapshots. |
| `EventStream` | State-free heterogeneous history, identity, versions, pending batch, checkpoint cursor, and duplicate index. `Append` accepts an event but applies no business rules. |
| `EventHandlerDispatcher<TState>` | Frozen exact-runtime-type registry; retains typed handlers for pure `Do` and `Undo`. |
| `IEventStreamStore` / `IAtomicEventStreamStore` | Atomic expected-version checks, whole-batch writes, validated histories, and consistent batch reads. |
| `ISnapshotStore<TState>` | Optional committed checkpoints containing complete root/entity state, rules, and schema identity. |

Core has no package dependencies and does not reference DI, compatibility, filesystem infrastructure,
databases, or Family Horizon. The separate `EventSourcing.DependencyInjection` package references
core and DI abstractions only. `EventSourcing.Compatibility` retains the obsolete reversible callback
lifecycle for existing integrations; it is not the recommended API for new applications.

## Immutable events and manual handler composition

Unrelated events can implement `IEvent` directly or use non-generic `EventBase`. No application event
base type is required. The optional `IEvent<TEvent>` / `EventBase<TEvent>` bridge also implements `IEvent`.

```csharp
using EventSourcing;
using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

public sealed record StockItem(string Sku, int Quantity) : IEntity;
public sealed record Inventory(string Name, StockItem Item);

public sealed class Received(Guid id, int quantity, EventMetadata? metadata = null)
    : EventBase(id, metadata ?? new(null, null))
{
    public int Quantity { get; } = quantity;
    public override IEvent Clone(EventMetadata metadata) => new Received(Id, Quantity, metadata);
}

public sealed class Renamed(Guid id, string previous, string next, EventMetadata? metadata = null)
    : EventBase(id, metadata ?? new(null, null))
{
    public string Previous { get; } = previous;
    public string Next { get; } = next;
    public override IEvent Clone(EventMetadata metadata) => new Renamed(Id, Previous, Next, metadata);
}

public sealed class ReceiptHandler : IEventHandler<Inventory, Received>
{
    public Inventory Do(Inventory s, Received e) => s with
    { Item = s.Item with { Quantity = checked(s.Item.Quantity + e.Quantity) } };
    public Inventory Undo(Inventory s, Received e) => s with
    { Item = s.Item with { Quantity = checked(s.Item.Quantity - e.Quantity) } };
}

public sealed class RenameHandler : IEventHandler<Inventory, Renamed>
{
    public Inventory Do(Inventory s, Renamed e) => s.Name == e.Previous
        ? s with { Name = e.Next } : throw new InvalidOperationException("Previous name mismatch.");
    public Inventory Undo(Inventory s, Renamed e) => s.Name == e.Next
        ? s with { Name = e.Previous } : throw new InvalidOperationException("Immediate resulting name required.");
}

public sealed class InventoryRoot : EventSourcedAggregate<Inventory>
{
    public InventoryRoot(Guid id, Inventory opening, EventHandlerDispatcher<Inventory> rules)
        : base(id, opening, rules) { }
    public InventoryRoot(EventSourcedAggregate<Inventory> restored) : base(restored) { }
    public override IEnumerable<IEntity> Entities => new IEntity[] { State.Item };
    public InventoryRoot Receive(Received e) => (InventoryRoot)Apply(e);
}
```

Compose handlers once at startup:

```csharp
var rules = EventHandlerDispatcher<Inventory>.CreateBuilder("inventory/v1")
    .Register<Received>(new ReceiptHandler()).Register<Renamed>(new RenameHandler()).Build();
var opening = new Inventory("Original", new StockItem("BOOK", 10));
var original = new InventoryRoot(aggregateId, opening, rules);
var changed = original.ApplyBatch(new IEvent[]
{
    new Received(receiptId, 5), new Renamed(renameId, "Original", "Updated")
});
var store = new InMemoryEventStreamStore();
var saved = await changed.SaveAsync(store);
// original remains at version zero. saved retains runtime type InventoryRoot, version two, no pending events.
```

IDs are nonempty GUIDs supplied by callers; they are separate from stream positions. All concrete
kinds share one ordered logical stream and contiguous one-based versions. Business dates stay in
payloads and never reorder history. Acceptance rejects recorded input metadata, null clones, changed
identity/type, and incorrect replacement metadata before reduction. The framework cannot prove clone
payload equivalence or deep immutability: event implementations must preserve both.

State, event payloads, derived-root fields, handlers and configuration must be deeply immutable.
Transitions must be pure and reentrant: no input mutation, I/O, clocks, generated IDs, mutable captured
root state, or ambient services. `Entities` projects complete authoritative `State`; derived roots must
not cache independent state or entity copies. Business invariants belong in handlers even when callers
use generic `Apply`. Unknown events and rejected batches publish no partial root.

Framework updates shallow-copy the runtime root and replace its private immutable state/stream frame.
Derived fields are shared under the immutability contract. A protected copy constructor accepts only
another consistent framework root; arbitrary state/history replacement is not a public operation.
Saved roots are prepared before persistence, and no reducer, subclass constructor, or user wrapper
callback executes after durable commit. Always retain the returned saved root. A superseded pending
branch saved again is stale. Read-only use of the original branch remains safe.

## Replay, provenance, and complete snapshots

```csharp
var definition = new ReplayDefinition<Inventory>(opening, rules.RuleVersion);
var full = EventSourcedAggregate<Inventory>.Replay(
    aggregateId, definition, rules, await store.ReadAsync(aggregateId));
var snapshots = new InMemorySnapshotStore<Inventory>();
await saved.SaveSnapshotAsync(snapshots, schemaVersion: 1);
var restored = new InventoryRoot(await EventSourcedAggregate<Inventory>.LoadAsync(
    aggregateId, definition, 1, rules, store, snapshots));
```

Persist original opening definitions consistently with history. Event stores intentionally store events
only. Rule identity covers implementation and immutable configuration; retain historical implementations
and select the recorded version explicitly. Mismatches fail before reduction. No automatic rule migration,
schema upgrade, or scenario reinterpretation is performed.

Snapshots contain complete root/entity state, aggregate ID, committed version, bound rules, and positive
state schema version. Capture rejects pending events, including branches whose event save failed.
An explicit snapshot label must match the bound rules. Snapshot writes are separate from event commits:
a failed checkpoint write cannot undo the committed ledger. Keep complete history and opening definitions.

Restoration selects the newest compatible checkpoint and reduces only its validated suffix. With no
compatible checkpoint, load replays the opening definition. Wrong identity/rules/schema, ahead-of-ledger
cursors, incomplete suffixes, corrupt storage, and duplicate IDs fail explicitly. Manually constructed
snapshots are an explicit trust boundary: cursor validation cannot prove arbitrary business data matches
history. Do not relabel schema or rules to bypass incompatibility.

`HistoryStartVersion` precedes retained `Events`; `Version` is the full accepted position, not suffix
length. Prefix IDs are still loaded to preserve duplicate detection before the checkpoint. Snapshot
restoration is therefore not constant-time, and complete business data may itself grow with history.

## Atomic saves and consistent multi-root restoration

```csharp
var committed = await EventSourcedAggregateBatch.SaveAsync(store,
    new IEventSourcedAggregate[] { householdAfterChange, differentlyTypedRoot });
// Results preserve each root's actual state/runtime type; no state-type coercion occurs.
```

The same-state overload returns `IReadOnlyList<EventSourcedAggregate<TState>>`. The heterogeneous overload
returns `IReadOnlyList<IEventSourcedAggregate>`; recover a known business type by a checked cast.
All saved roots are prepared before I/O. `EventStreamBatch.SaveAsync` provides state-free infrastructure
batching for streams only. Use the aggregate helper when saving business roots so pairs remain consistent.

Unchanged participants assert their expected versions as read dependencies. Duplicate stream participants
fail. The store validates the complete batch and publishes once; sequential independent saves are not an
atomic implementation. Duplicate IDs are scoped per stream, not globally. Applications must include every
affected root and enforce reconciliation; storage cannot infer omitted financial participants.

For restoration, select compatible checkpoints then call **one** `ReadBatchFromAsync` with all participant
cursors. Restore each typed root from its returned `StreamSlice`. Separate `LoadAsync` calls may observe
different commits. Memory and test file adapters verify whole-batch failure, concurrency, cancellation,
unchanged dependencies, and consistent reads. The memory store is thread-safe and non-durable; share one
instance per ledger. Cancellation before commit preserves data; adapters must report success after commit.

## Reversal and compensation

Each handler must satisfy `Undo(Do(state, event), event) == state` for valid inputs and document/enforce
its reversal preconditions. Replacement events carry previous values. `Undo` computes a state value only;
it does not return a normal root paired with unchanged history, remove events, decrement versions, or save.
Persisted cancellation appends a new compensating event with a new ID and causal reference to the original.
A historical rollback/reopening engine is outside this library's scope.

## Optional dependency injection

```csharp
using EventSourcing.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

services.AddEventHandlerDispatcher<Inventory>("inventory/v1", handlers => handlers
    .AddHandler<Received, ReceiptHandler>().AddHandler<Renamed, RenameHandler>());
using var provider = services.BuildServiceProvider(new ServiceProviderOptions
{ ValidateScopes = true, ValidateOnBuild = true });
using var scope = provider.CreateScope();
var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory>("inventory/v1");
```

Client libraries can expose `services.AddFamilyHorizonAccounts()` from a separate optional integration
module while their handlers reference core only. Registration is keyed by state and explicit rule version;
versions coexist without replacing each other. Duplicate mappings and rule registrations fail. Constructors
receive dependencies once when the dispatcher is built; `Do`/`Undo` retain handlers and make no container
lookups. Configuration freezes at startup. No assembly scanning is performed.

Dispatcher and handler lifetimes are explicit (scoped by default). Enable scope validation. Singleton
handlers/dispatchers cannot capture scoped dependencies; retained transient dependencies effectively live
for the dispatcher lifetime. Keep roots and their rules inside the required scope. Scope disposal rejects
further transitions; computed immutable state remains readable. See the [adapter guide](src/EventSourcing.DependencyInjection/README.md).

## Serialization and diagnostics

Durable adapters use stable domain discriminators and positive payload schema versions, not assembly-qualified
CLR names or uncontrolled runtime activation. API migration does not change event meaning or historical
formats. Unknown kinds/unsupported formats fail before reconstructed roots are published. Adapter migrations
and upcasters are explicit application responsibilities. Snapshot schemas are independently versioned.

Exceptions expose stream IDs, `ExpectedVersion`, `ActualVersion`, `EventId`, clone types, `EventType`, rules,
and schema identities so consumers need not parse messages. The file adapters are test fixtures, not a
production database or deployment selection.

## Build, package consumption, and performance

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx -c Release --no-restore -warnaserror
dotnet test EventSourcing.slnx -c Release --no-build --no-restore
```

Release builds generate all three 0.2.0 packages and portable symbols under ignored `artifacts/packages/`.
The [package-only consumers](validation/PackageConsumer/README.md), DI consumer, and compatibility sample
compile/run against generated packages without source project references. Core-only consumption has no
transitive DI dependency. Public APIs include XML IntelliSense documentation.

[BenchmarkDotNet performance tests](benchmarks/EventSourcing.PerformanceTests/README.md) run separately in
Release with allocation diagnostics, setup/IDs and correctness checks outside measured operations, and
recorded machine/runtime/source provenance. Correctness CI contains no timing thresholds.

GitHub release publishing remains an explicit later operation. Publishing a tag such as `v0.2.0` triggers
`.github/workflows/release.yml`, which builds/tests, publishes matching packages to GitHub Packages and
attaches packages/symbols. Never overwrite an existing published version. This local refactor does not
publish or update Family Horizon's package dependency; those steps follow validated release publication.
