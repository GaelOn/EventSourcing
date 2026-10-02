# Migrating to 0.2.0

This is a breaking source refactor with one supported core architecture. No public stateful generic stream
wrapper is retained. Version 0.1.0 packages remain immutable; publish 0.2.0 under a new release tag only
after the release gate passes. See the [design specification](aggregate-stream-refactor-specification.md).

| Previous API | Replacement |
| --- | --- |
| `EventSourcedAggregate<TState, TEvent>` | `EventSourcedAggregate<TState>` owns state and a matching `EventStream`. |
| `IEventSourcedAggregate<TState, TEvent>` | `IEventSourcedAggregate<TState>`; non-generic lifecycle interface supports heterogeneous batches. |
| `EventStream<TState, TEvent>` | State-free non-generic `EventStream`. |
| Stream `Apply`, `HandleAsync`, `Replay`, `LoadAsync` | Aggregate operations; stream `Append` applies no business rules. |
| `EventHandlerDispatcher<TState, TEvent>` / builder | State-only dispatcher/builder; `.Register<TConcreteEvent>(handler)` stays typed. |
| `IEventReducer<TState, TEvent>` / `EventReducer<TState, TEvent>` | State-only reducer consuming `IEvent`, with mandatory rule identity. |
| Required `IEvent<TEvent>` hierarchy | Non-generic `IEvent`; optional typed clone bridge remains for legacy consumers. |
| Generic store, append, slice and memory-store DTOs | Non-generic heterogeneous ports and views. |
| Stream state/rule/snapshot access | Aggregate owns `State`, `RuleVersion`, capture and snapshot save. |
| DI state/event registration and resolution | State-only APIs keyed by explicit rule identity. |
| Root wrapping `new(await Stream.SaveAsync(...))` | Framework `root.SaveAsync(...)` preserves runtime type and prepares the saved root before I/O. |
| Stateful-stream batch saving | `EventSourcedAggregateBatch.SaveAsync`; `EventStreamBatch` remains state-free infrastructure. |

Opening root constructors call `base(id, openingState, identifiedRules)`. Historical constructors accept
an already rehydrated `EventSourcedAggregate<TState>` and call the protected copy constructor. Do not
construct arbitrary state/history pairs. Project entities and business properties from `State`, never
cached independent copies. All additional subclass fields must be immutable; framework updates share them.
Business methods can cast `Apply`'s returned root to their own runtime type. Save results preserve that
same type and can be checked/cast by callers; no virtual wrapper factory is invoked after commit.

Delegate composition must supply an honest explicit identity, either `new EventReducer<TState>(version,
(state, e) => ...)` or the aggregate's explicit version/delegate overload. Existing typed delegates must
adapt `IEvent` to a checked concrete type; typed handler dispatch is preferable for unrelated types.
There is no unbound-delegate snapshot path. Supplying a conflicting snapshot label now always fails.
Tests/adapters needing an incompatible historical checkpoint construct it explicitly at the trusted
snapshot boundary rather than relabelling a live bound root.

`EventSourcing.Compatibility` continues its existing typed clone/callback lifecycle against matching
core 0.2.0. It still owns commit/effect/undo/rollback recovery and remains obsolete. The optional typed
bridge ensures clones reach the new non-generic validator without changing callback semantics. New
applications should not migrate onto this lifecycle.

Stable discriminator/payload formats in existing catalog and ledger test adapters remain unchanged.
Removing C# generic parameters does not imply a data migration. Real durable adapters must implement
heterogeneous decoding explicitly and preserve or explicitly reject historical formats; never infer CLR
names, silently change event meaning, or relabel rule/state schema versions.

The separate financial household fixture demonstrates unrelated income, transfer, expense, and closure
events in one ordered stream. It is acceptance-test data, not a new Family Horizon business implementation.
After publication, revise the FinancialState design and Family Horizon dependency in a separate follow-up.
