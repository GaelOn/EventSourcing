# EventSourcing 0.2.0

MIT-licensed standalone .NET 10 core with no NuGet dependencies. Business roots derive from
`EventSourcedAggregate<TState>` and own one complete immutable state paired with a state-free,
heterogeneous `EventStream`. Unrelated event classes implement `IEvent`; optional typed clone bridges
remain supported. `EventHandlerDispatcher<TState>` retains pure typed `Do`/`Undo` handlers.

Aggregate application/replay, rule-bound complete snapshots, optimistic concurrency, duplicate identity
validation, cancellation, immutable branching, atomic heterogeneous aggregate saves and consistent batch
reads preserve lifecycle guarantees. Stores are non-generic and infrastructure-independent. Derived-root
fields must be immutable; entity access projects State. Framework saved-root publication is prepared before I/O.

The optional DI and legacy compatibility packages depend on core; core references neither. Do not use
stream Append as a substitute for aggregate business reduction. Pure Undo computes state only; persisted
cancellation appends a compensating event and retains history.

See the [complete guide and examples](https://github.com/GaelOn/EventSourcing/blob/main/README.md),
[migration guide](https://github.com/GaelOn/EventSourcing/blob/main/docs/migration-0.2.0.md), and
[lifecycle contracts](https://github.com/GaelOn/EventSourcing/blob/main/docs/refinement-lifecycle.md).

## Transactional idempotency (0.3.0)

`OperationProcessor` uses an injectable `IEventTransactionCoordinator` with scoped
client-defined keys, isolated event plans and atomic consumed receipts. Duplicate
proposals never run consequences. Replay uses a selected-history admission view
and still applies each original historical event. Low-level event-ID persistence
is unchanged; it does not infer semantic keys. Optional JSON storage and DI remain
separate packages. See repository docs/transactional-idempotency.md.
