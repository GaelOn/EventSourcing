# EventSourcing.Compatibility

## Package and installation

Version **0.3.0** targets **.NET 10** and is licensed under MIT. Configure the [GitHub Packages NuGet feed](https://nuget.pkg.github.com/GaelOn/index.json) and an authorized package-reading credential before installing:

```bash
dotnet add package EventSourcing.Compatibility --version 0.3.0
```

Keep credentials out of source control. Public repository visibility does not imply anonymous package restore. Alternatively, build the repository in Release and use its generated packages as a local feed; see the [root guide](https://github.com/GaelOn/EventSourcing/blob/main/README.md).


Optional .NET 10 package containing the obsolete synchronous `CallbackEventSourcedAggregate` and its reversible transaction coordinator. New business roots should reference `EventSourcing` and derive from the state-owning `EventSourcedAggregate<TState>` with a state-free `EventStream`. The business aggregate concept is not obsolete.

This package depends on the matching `EventSourcing` core package. The core has no dependency on compatibility code. Existing `EventSourcing.ObjectsAggregation` and `EventSourcing.Transaction` namespaces are retained; add an explicit reference to this package when maintaining callback consumers on version `0.3.0`.

The earlier `0.1.0-alpha.3` prerelease renamed the old callback class to `CallbackEventSourcedAggregate` and its interface to `ICallbackEventSourcedAggregate`. `IAggregate` and `IEntity` now come from core. The callback lifecycle and recovery behavior are preserved; snapshots belong to the supported immutable root/stream path.

The shared event contract uses `EventMetadata` in namespace `EventSourcing`, `IEvent.Metadata`, and `EventMetadata.StreamId`. Update event constructors and `Clone(EventMetadata)` implementations accordingly. These event-contract changes originated in the earlier prerelease migration; they are not a new change introduced by 0.3.0.

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

## Migration and examples

For new code, prefer the immutable aggregate API in the core package. See the [migration guide](https://github.com/GaelOn/EventSourcing/blob/main/docs/migration-0.2.0.md), [root guide](https://github.com/GaelOn/EventSourcing/blob/main/README.md) and [compatibility package consumer](https://github.com/GaelOn/EventSourcing/blob/main/samples/EventSourcing.PackageConsumer/README.md). This package preserves a legacy integration boundary; it does not add distributed atomicity to callback side effects.

