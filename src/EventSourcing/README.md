# EventSourcing

Standalone .NET 10 library for immutable event streams, deterministic replay, and atomic multi-stream persistence contracts.

`EventStream<TState, TEvent>` is the preferred aggregate API. It provides immutable calculation, deterministic replay, and optimistic concurrency through storage ports. `EventStreamBatch` commits related streams atomically. `InMemoryEventStreamStore<TEvent>` is a thread-safe, non-durable implementation included in the package.

Legacy callback aggregates are available only in the optional `EventSourcing.Compatibility` package. The core package does not contain or reference that API. New financial logic should use immutable streams and atomic store operations. No production database or distributed transaction is selected.

## Identity and stream versions

Callers supply a nonempty `Guid` event `Id`. Cloning preserves it. `EventMetadata` carries `long? Version` and `Guid? StreamId`, exposed by `IEvent.Metadata`, independently of event identity. New events are unassigned; acceptance tags a copy with the stream ID and next one-based version. Duplicate IDs are rejected within one stream, including across stored history and a new batch. Separate streams may contain the same source ID (for example, a correlated transfer); use `EventStreamBatch` to persist their effects together.

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

History uses structurally shared immutable lists and identity sets. `Apply` no longer scans or copies all prior events, and saved views reuse history. The memory store validates only the incoming suffix against its stored identity index, stages every participant in a new immutable ledger, then publishes once. Full replay still reads and validates every event. Snapshots, history compaction, and automatic retries are not implemented.

## Replay provenance

An event stream alone is not a complete persisted scenario. Its opening inputs and the reducer's rules determine the result. Applications must:

1. Persist immutable opening inputs and a stable rule version under the stream identity. Include currency, effective dates, rates, tax tables, and other facts used by the reducer in the inputs/events or an immutable versioned reference.
2. Save that definition consistently with the corresponding history. Do not silently replace it when changing rules or opening inputs; create an explicitly versioned scenario or migration.
3. Retain and select the reducer implementing the recorded rule version.

`ReplayDefinition<TState>` represents the persisted opening state and rule version without depending on serialization or storage. Prefer the guarded replay overload for persisted scenarios:

```csharp
var definition = new ReplayDefinition<Balance>(new Balance(100m), "balance/v1");
var live = new EventStream<Balance, LedgerEvent>(streamId, definition.OpeningState, ReduceBalanceV1);
// The application saves definition and history together under streamId, then reloads both.
var replayed = EventStream<Balance, LedgerEvent>.Replay(
    streamId, storedDefinition, "balance/v1", ReduceBalanceV1, storedHistory);
```

A mismatched reducer version throws `ReplayRuleVersionException` before reduction. Version names are compared ordinally. The guard checks the supplied identity; it cannot prove what a delegate does, validate an application's choice of opening inputs, or preserve old rule implementations automatically. Applications remain responsible for honest reducer identities and loading the definition belonging to the stream. The low-level opening-state overload remains available for callers managing those checks themselves.

`IEventStreamStore` stores events only; it does not persist definitions. `ReplayDefinitionTests` demonstrates restoring a definition and history from one atomically replaced scenario document. The JSON ledger fixture remains an event-only storage adapter, not a complete scenario repository.

`StreamTests` provides a concrete decimal-balance example: opening 100, income 25, expense 10 reconciles to 115 after both live execution and replay. `StreamPersistenceTests` exercises a test-only `JsonLedgerStore` against real files, using a self-contained test `JsonFileStore` for exclusive writer leases and atomic document replacement. Tests cover reopening, stale writes, stored duplicates, invalid batches, cancellation, corrupt history, and writer failure followed by retry. The core NuGet package has no dependency on that infrastructure or on filesystem APIs.

## Thread safety and multi-stream transactions

- Immutable streams can be read, branched, and saved concurrently when state/events are immutable and the reducer is pure and reentrant. Generic constraints cannot enforce deep immutability. Branching does not mutate the original; competing saves are resolved by expected-version checks, not automatic merging.
- Share one `InMemoryEventStreamStore` instance per ledger. It serializes single-stream and batch reads/writes under one gate and publishes a new ledger only after every participant validates. It is process-local and non-durable; separate instances do not share data.
- `IAtomicEventStreamStore` extends the single-stream port with atomic `AppendBatchAsync` and consistent `ReadBatchAsync`. There is deliberately no fallback that loops over independent commits. Production adapters must provide one transaction covering all participants, including expected-version checks and duplicate detection.
- `EventStreamBatch.SaveAsync` checks each participant once and returns saved views only after success. Unchanged participants still assert their versions, allowing read dependencies to participate in optimistic concurrency. Failure leaves all input views and pending events unchanged. Duplicate stream entries are rejected.
- Use `ReadBatchAsync` when a household view must not observe half a transfer. Separate `ReadAsync` calls may observe different committed snapshots. Returned snapshots stay unchanged after subsequent writes.
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

Packages are written to the repository's ignored `artifacts/packages/` directory. The package IDs are `EventSourcing` and `EventSourcing.Compatibility`; both default to `0.1.0-alpha.2`. Release builds also generate portable-symbol `.snupkg` packages. Override the version when creating another release:

```sh
dotnet build EventSourcing.slnx -c Release -p:Version=0.1.0-alpha.3
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
