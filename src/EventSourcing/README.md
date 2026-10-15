# EventSourcing

Immutable business aggregates, deterministic replay and explicit persistence contracts for .NET 10. The core package has no external NuGet dependencies and references neither dependency injection nor filesystem adapters.

## Package and installation

Version **0.3.0** targets **.NET 10** and is licensed under MIT. Configure the [GitHub Packages NuGet feed](https://nuget.pkg.github.com/GaelOn/index.json) and an authorized package-reading credential before installing:

```bash
dotnet add package EventSourcing --version 0.3.0
```

Keep credentials out of source control. Public repository visibility does not imply anonymous package restore. Alternatively, build the repository in Release and use its generated packages as a local feed; see the [root guide](https://github.com/GaelOn/EventSourcing/blob/main/README.md).

## Model and responsibilities

- `EventSourcedAggregate<TState>` owns one authoritative immutable business state and its matching event stream.
- `EventStream` owns heterogeneous history, versions and pending events; appending to it does not execute business rules.
- `EventHandlerDispatcher<TState>` binds exact event types to pure, versioned `Do` and `Undo` handlers.
- Snapshot contracts support complete committed checkpoints, with explicit rule and schema identities.
- Store interfaces define optimistic concurrency, atomic multi-stream saves and consistent batch reads.
- `OperationProcessor` and transaction coordination support client-defined operation identities, staged plans and committed journal receipts.

Keep state, event payloads, handler configuration and derived-root fields deeply immutable. Handlers must be deterministic and free of I/O, clocks and ambient mutable dependencies. Applications own financial or other business invariants, original opening definitions and historical rule selection.

## Retry and recovery boundaries

Raw appends remain strict expected-version writes. Retrying a published raw append can throw a concurrency exception; an exception after publication does not prove that nothing was committed.

Semantic retries use explicit operation keys and journal receipts. A consumed operation returns its stored receipt without executing the proposal again. The library does not infer operation identity from payloads or compare arbitrary request content; applications must bind a key to the intended work and reject conflicting reuse where required.

The local coordinator's abandoned-claim recovery is explicit. Follow the documented ownership and recovery procedure before admitting work after reopening. Callback exceptions are propagated; consumers remain responsible for containing job failures and classifying storage errors.

## Snapshots and compensation

Snapshots are separate from event commits and do not replace the retained opening definition or complete history. Compatibility checks cover identity, cursor, rules and schema; they do not prove arbitrary supplied snapshot business data is authentic. Loading prefix identities means snapshot restoration is not constant-time.

Pure `Undo` calculates state only. It does not erase events or roll back durable history. Persisted cancellation requires a new compensating event.

## Optional packages

- `EventSourcing.DependencyInjection`: explicit startup registration of versioned handlers.
- `EventSourcing.Storage.Json`: exclusively owned local JSON event and journal storage.
- `EventSourcing.Compatibility`: legacy callback lifecycle for existing consumers.

## Examples and contracts

Start with the [complete core package consumer](https://github.com/GaelOn/EventSourcing/blob/main/validation/PackageConsumer/Program.cs), which exercises handlers, aggregates, snapshots, duplicate detection and atomic saves.

- [Architecture and examples](https://github.com/GaelOn/EventSourcing/blob/main/README.md)
- [Transactional idempotency](https://github.com/GaelOn/EventSourcing/blob/main/docs/transactional-idempotency.md)
- [Strict raw append contract](https://github.com/GaelOn/EventSourcing/blob/main/docs/raw-append-contract.md)
- [Migration from earlier APIs](https://github.com/GaelOn/EventSourcing/blob/main/docs/migration-0.2.0.md)
- [Lifecycle contracts](https://github.com/GaelOn/EventSourcing/blob/main/docs/refinement-lifecycle.md)

The core defines contracts, not a universal production database, distributed transaction manager or automatic schema migration system.

