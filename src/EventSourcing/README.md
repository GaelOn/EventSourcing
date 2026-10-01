# EventSourcing

Experimental .NET 10 aggregate event acceptance and commit abstractions, maintained in its own repository.

`EventStream<TState, TEvent>` is the preferred aggregate API. It provides immutable calculation, deterministic replay, and optimistic concurrency through storage ports. `EventStreamBatch` commits related streams atomically. `InMemoryEventStreamStore<TEvent>` is a thread-safe, non-durable implementation included in the package.

The callback-based `EventSourcedAggregate` is marked obsolete and retained only for existing synchronous reversible integrations. New financial logic should use immutable streams and atomic store operations. The callback coordinator is not a database transaction and must not be used to simulate a multi-stream commit with sequential callbacks. No production database or distributed transaction is selected.

## Identity and stream versions

Callers supply a nonempty `Guid` event `Id`. Cloning preserves it. `TransactionnalData` now carries a nullable `long Version` and aggregate ID, not an allocated event ID. New events are unassigned; acceptance tags a copy with the stream ID and next one-based version. Duplicate IDs are rejected within one stream, including across stored history and a new batch. Separate streams may contain the same source ID (for example, a correlated transfer); use `EventStreamBatch` to persist their effects together.

This is a breaking prerelease change: `EventBase` constructors now require the caller ID, `IAggregate` exposes `Version`, and `IEventIdSource` has been removed. The legacy callback aggregate retains its transaction lifecycle and rejects duplicate IDs before invoking callbacks. Its `Events` collection must represent the full accepted history for its count-based version to be meaningful.

## Immutable calculation, replay, and persistence

`EventStream<TState, TEvent>` is the calculation and storage path. Supply immutable state/events and a pure reducer `(state, event) => newState`. The reducer must not mutate its arguments, generate IDs, read ambient time, or perform I/O.

- `Apply(event)` returns a new stream with updated state and pending history. The original is unchanged, even when reduction fails.
- `Replay(id, openingState, reducer, history)` validates stream identity, contiguous versions, and unique IDs before reducing history in version order. It never invokes the transaction manager, persistence, or external side-effect callbacks. Financial effective dates remain event payload; they do not reorder stream membership.
- `SaveAsync(store)` sends the pending batch with `PersistedVersion`. Only success returns a new stream marked as saved. Failure preserves the original stream and its pending events. Reuse the returned saved stream; retrying an older stream after success is stale.
- `HandleAsync(event, store)` calculates first, then saves. Storage failure cannot publish a changed aggregate. There is no post-save reducer or user callback that could fail after the durable commit.
- `IEventStreamStore<TEvent>` implementations must validate the entire append batch, reject stale versions and duplicate IDs, and preserve storage on failure. After a stale write, reload and reconcile rather than blindly retrying.

This initial implementation retains full history and replays from opening state. Snapshots, history compaction, and automatic retries are not implemented.

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

## Compatibility transaction contract

- Preparation must be free of observable changes. Preparation errors propagate before transaction execution.
- Commit runs before application. Returning `TransactionFailure` means commit rejected the operation without changing state; that exact failure is returned without apply, undo, or rollback.
- A successful commit is followed by application. Successful application returns `TransactionSuccess`.
- Failed or throwing application triggers `Undo`, then commit rollback. Undo must restore even partially applied changes. Rollback is attempted even when undo fails.
- A throwing commit may have partially changed state, so rollback is attempted without applying or undoing the side effect.
- Aggregate subclasses must implement `RollbackEvent` to restore committed state and history. Clearing metadata alone is insufficient. Rollback must tolerate a commit that threw before changing anything. A successful rollback returns `TransactionRollbacked`.
- Successful recovery returns the original failure. Recovery errors return `TransactionFailure` with an `AggregateException` containing the original cause first, followed by undo and rollback errors in order. Incomplete recovery requires reconciliation before retrying.
- Null or unsupported callback results are treated as contract failures, with recovery attempted where needed.

The `ref` rollback callback is retained for compatibility. Reassigning its event changes only the transaction's local reference, not caller references or stored history. Recovery must restore the actual resources owned by the aggregate or adapter. Durable changes require a rollback-capable storage transaction or an explicit compensation implementation; this coordinator cannot make independent writes atomic.

Compatibility aggregate `AcceptEvent` calls are serialized per instance, and reentrant acceptance is rejected. This protects the acceptance lifecycle only: subclasses must not mutate state/history outside it or expose mutable data to concurrent readers. Callbacks must not wait for another thread to reenter the same aggregate. Standalone transaction scopes also serialize execution per instance and reject reentrancy. Side-effect objects must not be shared across different scopes without their own coordination. Prefer immutable streams for concurrent applications. Caller-supplied identities and stream positions are separate from callback recovery.

## Build and test

From the repository root:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx --no-restore
dotnet test EventSourcing.slnx --no-build
```

The solution references the library and `src/Test/EventSourcing.Tests`. File-backed tests use a local test fixture; no Family Horizon projects are referenced.

## Generate a NuGet package

Release builds generate the package automatically:

```sh
dotnet build EventSourcing.slnx -c Release
dotnet test EventSourcing.slnx -c Release --no-build
```

Packages are written to the repository's ignored `artifacts/packages/` directory. The default package ID is `EventSourcing` and the default version is `0.1.0-alpha`. Override the version when creating another release:

```sh
dotnet build EventSourcing.slnx -c Release -p:Version=0.1.0-alpha.2
```

To package an already built Release library explicitly:

```sh
dotnet pack src/EventSourcing/EventSourcing.csproj -c Release --no-build
```

Package generation stays local; no feed publication or credentials are configured. A public package ID and license must be selected before public distribution.
