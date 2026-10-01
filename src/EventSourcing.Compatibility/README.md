# EventSourcing.Compatibility

Optional .NET 10 package containing the obsolete synchronous `CallbackEventSourcedAggregate` and its reversible transaction coordinator. New business roots should reference `EventSourcing` and compose an `EventStream<TState, TEvent>` through the supported core `EventSourcedAggregate`. The business aggregate concept is not obsolete.

This package depends on the matching `EventSourcing` core package. The core has no dependency on compatibility code. Existing `EventSourcing.ObjectsAggregation` and `EventSourcing.Transaction` namespaces are retained; add an explicit package reference when migrating callback consumers to `0.1.0-alpha.3`.

`0.1.0-alpha.3` renames the old callback class to `CallbackEventSourcedAggregate` and its interface to `ICallbackEventSourcedAggregate`. `IAggregate` and `IEntity` now come from core. The callback lifecycle and recovery behavior are preserved; snapshots belong to the supported immutable root/stream path.

The shared event contract uses `EventMetadata` in namespace `EventSourcing`, `IEvent.Metadata`, and `EventMetadata.StreamId`. Update event constructors and `Clone(EventMetadata)` implementations accordingly. This is a breaking prerelease change.

## Callback contract

- Preparation must make no observable changes.
- Commit precedes application. An explicit `TransactionFailure` from commit means no changes were made and no recovery is needed.
- A throwing commit triggers rollback without applying or undoing the side effect.
- Failed or throwing application triggers `Undo`, then commit rollback. Undo must restore partial application; rollback is attempted even when undo fails.
- Aggregate subclasses must restore committed resources and history in `RollbackEvent`, including when commit threw before changing anything.
- Successful recovery returns the original failure. Incomplete recovery returns an `AggregateException` with the original error first, then undo and rollback errors.
- Null and unsupported callback results are failures.

The coordinator cannot make independent durable writes atomic. Use a real storage transaction or explicit compensation for durable effects. Use `EventStreamBatch` for multi-stream persistence; never substitute sequential callback commits.

Acceptance is serialized per aggregate, and execution is serialized per scope. Reentrancy is rejected. Subclasses must protect mutable state/history outside acceptance and must not wait for another thread to reenter the same aggregate. Side effects shared across scopes need their own coordination. Full history must remain available for the count-based aggregate version.

The `ref` rollback callback only reassigns a local event reference. Recovery must restore resources owned by the aggregate or adapter, not merely clear event metadata.
