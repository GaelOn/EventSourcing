# EventSourcing

Standalone .NET 10 library for immutable event streams, typed business state handlers, deterministic replay, and atomic multi-stream persistence contracts.

`EventSourcedAggregate<TState, TEvent>` is a supported business-root abstraction in the core package. It composes an `EventStream<TState, TEvent>` for immutable calculation, deterministic replay, and optimistic concurrency through storage ports. `EventStreamBatch` commits related streams atomically. `InMemoryEventStreamStore<TEvent>` is a thread-safe, non-durable implementation included in the package.

The legacy `CallbackEventSourcedAggregate` lifecycle is available only in the optional `EventSourcing.Compatibility` package. The core package does not contain or reference that API. New financial logic should use immutable streams and atomic store operations. No production database or distributed transaction is selected.

## Identity and stream versions

Callers supply a nonempty `Guid` event `Id`. Cloning preserves it. `EventMetadata` carries `long? Version` and `Guid? StreamId`, exposed by `IEvent.Metadata`, independently of event identity. New events are unassigned; acceptance tags a copy with the stream ID and next one-based version. Duplicate IDs are rejected within one stream, including across stored history and a new batch. Separate streams may contain the same source ID (for example, a correlated transfer); use `EventStreamBatch` to persist their effects together.

## Business aggregate roots

The aggregate root owns the business meaning, invariants, and entity access. The stream owns update mechanics and persistence positions. The core `EventSourcedAggregate<TState, TEvent>` is not obsolete and imposes no commit/apply/undo callbacks.

Derive a business root from it, pass an immutable stream to its constructor, expose entities from `State`, and wrap changed/saved streams in a new root. `State` delegates to the composed stream, so there is one authoritative state rather than two competing copies. Typed business methods and reducer invariants remain in the derived root; the generic base cannot infer domain rules.

`State` must contain the complete immutable data needed to restore the root and every owned entity. Do not snapshot a live root with services, storage references, reducers, or callbacks. `AggregateSnapshotTests` demonstrates root name, account entities, currency, and balance surviving file-backed restoration without replaying old events.

## Refinements in 0.1.0-alpha.5

- Handlers implement pure `Do` and `Undo`. `Handle` forwards to `Do`; previous implementations must provide both transitions. Events carry reversal data, including previous values for replacements. Validate domain preconditions and test immediate reversal. Pure reversal does not edit any stream; persisted cancellation appends a new compensating event and retains history.
- `IEventReducer` binds `RuleVersion` to a calculation. `EventReducer` wraps a pure delegate; the dispatcher implements the same interface and requires a rule version at construction. Pass the calculation object to streams and versioned replay/load overloads rather than detaching its delegate.
- Snapshot capture with `CaptureSnapshot(schemaVersion)` derives the bound identity. Explicit labels cannot relabel a bound stream. Low-level unbound delegate/string APIs remain for compatibility; automatic capture on an unbound stream fails. Rule identities include handler configuration and must be assigned honestly by the application.
- Public aggregate `Stream` access is intentional for immutable branching, batching, and composition. Business operations are the application's normal entry point; applications requiring restricted access supply their own façade. `HistoryStartVersion`, `PendingEvents`, `HasPendingChanges`, and `RuleVersion` expose lifecycle state.
- Clone validation rejects a changed concrete type before reduction. Exceptions expose event IDs, expected/actual versions, stream IDs, rule identities, schema versions, and clone types for structured diagnostics.
- NuGet packages include XML API documentation. The separate BenchmarkDotNet project measures time and allocations; it does not add dependencies to core or timing assertions to correctness CI.

See the [complete lifecycle contracts](../../docs/refinement-lifecycle.md) and [performance methodology](../../benchmarks/EventSourcing.PerformanceTests/README.md). The heterogeneous JSON test adapter demonstrates stable event discriminators and payload versions; unsupported formats fail explicitly. It remains a test fixture.

## Migration to 0.1.0-alpha.3

- `EventSourcedAggregate<TState, TEvent>`, `IAggregate`, `IEntity`, and `IEventSourcedAggregate` are core business abstractions in `EventSourcing.ObjectsAggregation`.
- The callback implementation is now `CallbackEventSourcedAggregate<TState, TEvent>` with `ICallbackEventSourcedAggregate`, in the compatibility package. Callback consumers must update their base/interface names. Only the legacy callback lifecycle is obsolete.
- The supported root takes an `EventStream` constructor argument. Its `IEventSourcedAggregate` exposes non-null business `State` and the composed `Stream`.
- Storage adapters must implement `ReadFromAsync` and, for atomic stores, `ReadBatchFromAsync`. There is no fallback that downloads full historical payloads or loops over independent reads.
- `Events` now means retained history after `HistoryStartVersion`. Opening-state streams still retain all history from version zero. After snapshot restoration, `Version` and `PersistedVersion` remain full stream positions and must never be inferred from `Events.Count`.

## Migration to 0.1.0-alpha.2

This prerelease introduces breaking API changes:

- Replace `EventSourcing.Transaction.TransactionnalData` with `EventSourcing.EventMetadata`.
- Replace `IEvent.TransactionnalData` with `IEvent.Metadata`, and `AggregateId` on metadata with `StreamId`.
- Update constructors and `Clone(EventMetadata metadata)` implementations; payload and caller ID must remain unchanged when cloning.
- Callback consumers must add an explicit `EventSourcing.Compatibility` package reference. Their `EventSourcing.ObjectsAggregation` and `EventSourcing.Transaction` namespaces are retained. The compatibility package depends on core, never the reverse.
- Event identities remain nonempty caller-supplied GUIDs; versions remain one-based stream positions.

## Immutable calculation, replay, and persistence

`EventStream<TState, TEvent>` is the calculation and storage path. Supply immutable state/events and a pure reducer `(state, event) => newState`. The reducer must not mutate its arguments, generate IDs, read ambient time, or perform I/O.

- `Apply(event)` returns a new stream with updated state and pending history. The original is unchanged, even when reduction fails.
- `Replay(id, openingState, reducer, history)` validates stream identity, contiguous versions, and unique IDs before reducing history in version order. It never invokes the transaction manager, persistence, or external side-effect callbacks. Financial effective dates remain event payload; they do not reorder stream membership.
- `SaveAsync(store)` sends the pending batch with `PersistedVersion`. Only success returns a new stream marked as saved. Failure preserves the original stream and its pending events. Reuse the returned saved stream; retrying an older stream after success is stale.
- `HandleAsync(event, store)` calculates first, then saves. Storage failure cannot publish a changed aggregate. There is no post-save reducer or user callback that could fail after the durable commit.
- `IEventStreamStore<TEvent>` implementations must validate the entire append batch, reject stale versions and duplicate IDs, and preserve storage on failure. After a stale write, reload and reconcile rather than blindly retrying.

History uses structurally shared immutable lists and identity sets. `Apply` no longer scans or copies all prior events, and saved views reuse history. The memory store validates only the incoming suffix against its stored identity index, stages every participant in a new immutable ledger, then publishes once. Full replay still reads and validates every event. Snapshots are optional checkpoints; deleting event history, automatic checkpoint scheduling, and automatic retries are not implemented.

## Typed event handlers

`EventSourcing.Handlers.IEventHandler<TState, TEvent>` expresses a pure state transition through `TState Do(TState state, TEvent @event)` and `TState Undo(TState state, TEvent @event)`. Use composition to give an aggregate several typed handlers, or implement several interfaces on one business object. Each handler must use its supplied state rather than reading a captured root instance.

```csharp
var dispatcher = EventHandlerDispatcher<InventoryState, WarehouseEvent>.CreateBuilder("warehouse/v1")
    .Register<StockReceived>(receivedHandler)
    .Register<StockShipped>(shippedHandler)
    .Build();
var stream = new EventStream<InventoryState, WarehouseEvent>(
    aggregateId, openingState, dispatcher);
```

Import `EventSourcing.Handlers`. Concrete events derive from the common stream event type and preserve their runtime type, identity, and payload in `Clone`. See the [repository README](../../README.md#compose-typed-event-handlers) for complete types and registration examples.

The dispatcher matches exact runtime types with one handler per registration. Duplicate registration throws `ArgumentException`; missing registration throws `UnhandledEventException`, exposing `EventType`. There is no base/interface fallback or silently ignored event. Null inputs and null results fail; business exceptions propagate. Existing immutable stream failure guarantees apply.

Each `Build()` captures an immutable map; later builder edits cannot alter built dispatchers. Builders are not thread-safe. Dispatchers can be shared when their retained handler instances are pure and reentrant, and state/events are deeply immutable. No I/O, mutable captured aggregate state, or command dispatch is introduced.

Application DI or startup configuration can supply handler instances to `Register`; core has no DI dependency and performs no service lookup during reduction. Retain dependencies for the dispatcher's lifetime. Freeze configuration and version the handler set/parameters with the persisted `RuleVersion`. The same dispatcher acts as reducer for live application, full replay, and snapshot suffix replay. This additive feature is available in `0.1.0-alpha.4`; existing delegate reducers remain supported.

## Replay provenance

An event stream alone is not a complete persisted scenario. Its opening inputs and the reducer's rules determine the result. Applications must:

1. Persist immutable opening inputs and a stable rule version under the stream identity. Include currency, effective dates, rates, tax tables, and other facts used by the reducer in the inputs/events or an immutable versioned reference.
2. Save that definition consistently with the corresponding history. Do not silently replace it when changing rules or opening inputs; create an explicitly versioned scenario or migration.
3. Retain and select the reducer implementing the recorded rule version.

`ReplayDefinition<TState>` represents the persisted opening state and rule version without depending on serialization or storage. Prefer the guarded replay overload for persisted scenarios:

```csharp
var definition = new ReplayDefinition<Balance>(new Balance(100m), "balance/v1");
var live = new EventStream<Balance, LedgerEvent>(streamId, definition.OpeningState,
    new EventReducer<Balance, LedgerEvent>("balance/v1", ReduceBalanceV1));
// The application saves definition and history together under streamId, then reloads both.
var replayed = EventStream<Balance, LedgerEvent>.Replay(
    streamId, storedDefinition, "balance/v1", ReduceBalanceV1, storedHistory);
```

A mismatched reducer version throws `ReplayRuleVersionException` before reduction. Version names are compared ordinally. The guard checks the supplied identity; it cannot prove what a delegate does, validate an application's choice of opening inputs, or preserve old rule implementations automatically. Applications remain responsible for honest reducer identities and loading the definition belonging to the stream. The low-level opening-state overload remains available for callers managing those checks themselves.

`IEventStreamStore` stores events only; it does not persist definitions. `ReplayDefinitionTests` demonstrates restoring a definition and history from one atomically replaced scenario document. The JSON ledger fixture remains an event-only storage adapter, not a complete scenario repository.

`StreamTests` provides a concrete decimal-balance example: opening 100, income 25, expense 10 reconciles to 115 after both live execution and replay. `StreamPersistenceTests` exercises a test-only `JsonLedgerStore` against real files, using a self-contained test `JsonFileStore` for exclusive writer leases and atomic document replacement. Tests cover reopening, stale writes, stored duplicates, invalid batches, cancellation, corrupt history, and writer failure followed by retry. The core NuGet package has no dependency on that infrastructure or on filesystem APIs.

## Snapshots

`AggregateSnapshot<TState>` associates complete immutable business state with `AggregateId`, `StreamVersion`, `RuleVersion`, and a positive `SchemaVersion`. The cursor is a stream position, not the last event GUID. Version zero represents the original opening state; applications must persist that opening definition independently.

Capture from a saved root or stream with no pending events. Capture rejects pending changes, including a branch whose event save failed. Checkpoint saving is a separate operation from committing events:

```csharp
var savedRoot = await changedRoot.SaveAsync(eventStore); // Typed wrapper supplied by the business root.
await savedRoot.SaveSnapshotAsync(snapshotStore, 1);
// If checkpoint saving fails, savedRoot and the committed event ledger remain valid.
```

`ISnapshotStore<TState>` returns the newest checkpoint matching aggregate identity, rule version, and schema version. Older/equal checkpoint writes cannot replace a newer entry for that same key. Rules and state schema have distinct versions: changing business rules does not automatically mean the serialized state shape changed. `InMemorySnapshotStore` is a thread-safe process-local implementation. The file-backed `JsonSnapshotStore` is a test fixture only.

Load from a compatible checkpoint and apply only its later committed events:

```csharp
var restoredStream = await EventStream<HouseholdState, HouseholdEvent>.LoadAsync(
    aggregateId, storedOpeningDefinition, 1,
    new EventReducer<HouseholdState, HouseholdEvent>("household/v1", ReduceHouseholdV1),
    eventStore, snapshotStore, cancellationToken);
var restoredRoot = new HouseholdAggregate(restoredStream);
```

No compatible checkpoint means replay from the persisted opening definition. Snapshot storage/corruption errors propagate rather than being silently hidden; retained events allow an explicit recovery through full replay. A checkpoint ahead of the ledger, mismatched identity/cursor, incomplete suffix, duplicate identities, or incompatible direct restoration is rejected before business reduction. The cache trusts callers to provide committed, valid business state; it cannot prove a manually constructed snapshot matches its history or that a reducer implements its supplied rule identity.

`ReadFromAsync(id, afterVersion)` returns a validated `StreamSlice`: only later event payloads, the current committed version, and earlier event IDs from one storage view. Earlier IDs are still loaded to preserve immediate duplicate rejection during `Apply`, including before the checkpoint. This index costs memory and work proportional to prior event count, but no earlier business payloads need to cross the port or be reduced. Durable adapters must retain their full identity index and atomically reject duplicates during writes.

For a household spanning several roots, select compatible checkpoints, then issue **one** `ReadBatchFromAsync` for every participant cursor and restore each root from its returned slice. Individual `LoadAsync` calls can observe different ledger commits and do not form a consistent household view. Checkpoint versions may differ; the batch suffix read brings every participant to one consistent storage view. `SnapshotPersistenceTests` verifies transfer reconciliation, and `MultiStreamTests` races batch suffix reads against transfers.

The JSON fixtures still read their complete documents; snapshots there avoid old reducer calls but do not provide database-style range I/O. Production range-read performance depends on the chosen durable adapter. Full event history and opening definitions remain available for diagnostics, rebuilding, and explicit migration; snapshots never delete them.

## Thread safety and multi-stream transactions

- Immutable streams can be read, branched, and saved concurrently when state/events are immutable and the reducer is pure and reentrant. Generic constraints cannot enforce deep immutability. Branching does not mutate the original; competing saves are resolved by expected-version checks, not automatic merging.
- Share one `InMemoryEventStreamStore` instance per ledger. It serializes single-stream and batch reads/writes under one gate and publishes a new ledger only after every participant validates. It is process-local and non-durable; separate instances do not share data.
- `IAtomicEventStreamStore` extends the single-stream port with atomic `AppendBatchAsync` and consistent `ReadBatchAsync` and `ReadBatchFromAsync`. There is deliberately no fallback that loops over independent commits. Production adapters must provide one transaction covering all participants, including expected-version checks and duplicate detection.
- `EventStreamBatch.SaveAsync` checks each participant once and returns saved views only after success. Unchanged participants still assert their versions, allowing read dependencies to participate in optimistic concurrency. Failure leaves all input views and pending events unchanged. Duplicate stream entries are rejected.
- Use `ReadBatchAsync` or `ReadBatchFromAsync` when a household view must not observe half a transfer. Separate `ReadAsync` calls may observe different committed snapshots. Returned snapshots stay unchanged after subsequent writes.
- Cancellation before commit leaves all streams unchanged. Once committed, an adapter must report success rather than a late cancellation. Retry requires reloading and reconciling stale versions; a file lease conflict requires retrying the complete operation.

For related streams using a common event type and state type:

```csharp
var saved = await EventStreamBatch.SaveAsync(store,
    new[] { checkingAfterTransfer, savingsAfterTransfer }, cancellationToken);
var history = await store.ReadBatchAsync(
    new[] { saved[0].Id, saved[1].Id }, cancellationToken);
```

The typed helper uses a common state/event type. Aggregates with different state types can construct `StreamAppend<TEvent>` requests directly using a shared event abstraction. Batches must belong to one store's transaction boundary; cross-database atomicity is not provided. The caller/domain must include every account affected by a transfer and validate financial reconciliation. Generic storage cannot infer omitted participants or financial meaning.

The JSON integration adapter now stores all test streams in one schema-v2 document, checks all versions under one file lease, and replaces the document once. Separate adapter instances targeting the same path coordinate through that lease; a competing writer may fail with `IOException`. This is a test adapter, with no migration promise for old test files. Both memory and file adapters are tested for atomic transfers, stale participants, duplicate IDs, cancellation, competing writers, consistent reads, and preserved snapshots.

## Compatibility callbacks

Use `EventSourcing.Compatibility` only for existing synchronous reversible integrations. Its callback coordinator cannot make sequential durable writes atomic. The package README documents preparation, undo/rollback, recovery failures, serialization, and reentrancy contracts. Multi-stream persistence belongs in `EventStreamBatch`.

## Build and test

From the repository root:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx --no-restore
dotnet test EventSourcing.slnx --no-build
```

The solution builds core, the optional compatibility package, the diagnostic benchmark, and `src/Test/EventSourcing.Tests`. File-backed tests use a local test fixture; no Family Horizon projects are referenced.

## Generate a NuGet package

Release builds generate the package automatically:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet test EventSourcing.slnx -c Release --no-build
```

Packages are written to the repository's ignored `artifacts/packages/` directory. The package IDs are `EventSourcing` and `EventSourcing.Compatibility`; both default to `0.1.0-alpha.5`. Release builds also generate portable-symbol `.snupkg` packages. Override the version when creating another release:

```sh
dotnet build EventSourcing.slnx -c Release -p:Version=0.1.0-alpha.6
```

To package an already built Release library explicitly:

```sh
dotnet pack src/EventSourcing/EventSourcing.csproj -c Release --no-build
```

Package generation stays local; no feed publication or credentials are configured. A public package ID and license must be selected before public distribution.

## Performance diagnostic

Run the dependency-free benchmark in Release mode:

```sh
dotnet run --project benchmarks/EventSourcing.Benchmarks -c Release --no-build
```

It reports CSV timings and process allocations for 1,000, 5,000, and 10,000 events, both pure application and saving each event to memory. Event IDs are prepared before measurement. Each result checks balance, version, and persisted event count. These are diagnostics, not timing assertions or production service-level guarantees; see the benchmark README for the measured baseline and limitations.
