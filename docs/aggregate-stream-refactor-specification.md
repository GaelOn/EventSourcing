# EventSourcing Aggregate and Stream Refactor Specification

Design specification · 1 October 2026

## 1. Objective

Refactor the public core API so a business aggregate owns its immutable state and can handle several unrelated concrete event types without declaring a common event type parameter on the aggregate or stream.

The target public abstractions are:

```csharp
EventSourcedAggregate<TState>
EventStream
EventHandlerDispatcher<TState>
IEventHandler<TState, TEvent>
IEvent
```

The stream contains event history and persistence lifecycle data. The aggregate contains the single authoritative business state and uses the dispatcher to compute transitions. Concrete handlers retain compile-time event typing.

This is a breaking refactor of the existing library, not merely a convenience wrapper. It must preserve deep immutability, deterministic replay, immediate handler reversal, optimistic concurrency, atomic batches, snapshots, rule provenance, and optional DI composition.

The existing `TEvent` parameter already permits multiple concrete events under a shared base type. The refactor removes the requirement for that base type and relocates state reduction from the stream to the aggregate. Both improvements must be demonstrated with unrelated event classes defined in an external consumer assembly.

## 2. Responsibility boundaries

| Component | Owns or performs | Excludes |
| --- | --- | --- |
| `EventSourcedAggregate<TState>` | Authoritative state, bound transition rules, entity access, apply/replay orchestration, snapshots, business-root lifecycle. | A second mutable state, event-store implementation, domain-specific rules inferred by the framework. |
| `EventStream` | Identity, globally ordered heterogeneous events, accepted/persisted positions, pending events, checkpoint boundary, duplicate identity index, append/save mechanics. | Business state, reducers, handler dispatch, snapshots of business state. |
| `EventHandlerDispatcher<TState>` | Immutable exact-runtime-type registry, pure forward/reverse dispatch, transition rule identity. | Event routing to independently ordered type-specific histories, container lookups during transitions. |
| `IEventHandler<TState, TEvent>` | Typed pure `Do` and `Undo` transitions. | Persistence, ambient clock/ID generation, mutable captured aggregate state. |
| Event and snapshot stores | Durable atomic operations and explicit serialization contracts. | Financial/business semantics or automatic rule selection. |

Core remains independent of DI frameworks, filesystem infrastructure, databases, UI, and Family Horizon.

## 3. One ordered logical stream per aggregate

One aggregate uses one logical stream identified by its aggregate GUID. All accepted concrete event types share one contiguous version sequence and one pending batch.

For example, an income, a transfer, an expense, and a month closure may occupy positions 1–4 in that order. Replaying those events must preserve that order regardless of their types or business dates.

A per-type index or filtered view is allowed for inspection. It must not create separate authoritative versions, histories, or state transitions. Physical storage partitioning by event kind is an adapter detail and must still provide the logical ordered stream and atomic append guarantees.

An optional registry/catalog above several streams must identify them by aggregate/stream ID. Event type selects a handler; it does not uniquely identify the target aggregate. The initial refactor does not require a global in-memory catalog of every application's stream.

If a later design introduces independently persisted type-specific streams for one root, it requires a new specification for aggregate-wide ordering, expected versions, checkpoint cursors, and atomic publication. That additional architecture is outside this refactor.

## 4. Non-generic event contract

Introduce a public non-generic event contract with the following essential shape:

```csharp
public interface IEvent
{
    Guid Id { get; }
    EventMetadata Metadata { get; }
    IEvent Clone(EventMetadata metadata);
}
```

This is the proposed API contract. Final implementation details must compile and be exercised by package-consumer tests.

Provide a non-generic `EventBase` convenience class, or another documented way for consumer events to implement the contract directly. Consumers must be able to declare unrelated `IncomeReceived`, `MoneyTransferred`, and `MonthClosed` classes without an application-specific common event base.

Typed `IEvent<TEvent>` / `EventBase<TEvent>` convenience forms may remain as an optional bridge. If retained, they must also satisfy non-generic `IEvent` and provide a correct non-generic clone path. The generic form must not remain a requirement for core streams, stores, roots, or dispatcher registration.

### 4.1 Event acceptance

Preserve existing requirements:

- A nonempty caller-supplied event GUID independent of stream position.
- Unassigned metadata for new events and explicit validation of supplied metadata.
- Clone acceptance tagging with the correct stream ID and next one-based version.
- A clone preserving exact concrete runtime type, identity, and deeply immutable payload.
- Rejection of null clones, changed concrete types, changed identities, and incorrect replacement metadata before business reduction.
- Duplicate detection across accepted history, pending events, and restored checkpoint prefixes.

The framework can validate identity/type/metadata but cannot generically prove payload equivalence or deep immutability. Document this consumer contract and test it with concrete examples; do not advertise an impossible generic guarantee.

`EventMetadata` retains its current meaning. Business dates and domain correlations remain payload data. The library must not reorder stream membership by effective date or generate event identities automatically.

## 5. State-free EventStream

`EventStream` is a deeply immutable, non-generic event-history and commit view. It must expose the equivalent of the current lifecycle information:

- `Id`.
- `Version`, including accepted pending events.
- `PersistedVersion`, the last successful durable position.
- `HistoryStartVersion`, preceding the retained suffix.
- `Events : IReadOnlyList<IEvent>`.
- `PendingEvents : IReadOnlyList<IEvent>`.
- `HasPendingChanges`.

It maintains the identity information needed for immediate duplicate rejection, including IDs preceding a restored checkpoint. It must preserve structural sharing rather than reintroducing full-history scans/copies on every append.

The stream has no `State`, `TState`, reducer, dispatcher, or business `RuleVersion`. It does not capture `AggregateSnapshot<TState>` or validate transition rule compatibility. Those responsibilities move to the aggregate layer.

Appending accepts and tags an immutable event without business reduction. Use an operation name such as `Append` or an internal acceptance operation so it is clear that the stream alone does not apply domain rules.

Stream validation/restoration must check stream identity, contiguous versions, duplicate IDs, cursor bounds, and suffix completeness. Saving sends the pending event batch with the expected persisted position and returns a new saved stream. Failures preserve the original view and its pending events.

Low-level stream/store operations are infrastructure capabilities, not a way to claim an aggregate's state has been reduced. Normal business consumers work through their aggregate root.

## 6. EventSourcedAggregate<TState>

Provide the supported business base class `EventSourcedAggregate<TState>` and update `IEventSourcedAggregate<TState>` accordingly. `TState` remains a non-null reference type and must be deeply immutable. No `IState` marker is required by this refactor.

Retain `IAggregate` and the `IEntity` marker. The application defines entity identity, ownership, traversal, and business invariants. `Entities` exposes entities projected from the authoritative state.

### 6.1 State ownership and state/stream consistency

The aggregate owns one `TState` value, one stream view, and one bound transition mechanism. It must never expose a root whose state corresponds to a different event prefix from its stream.

Use an immutable internal state/stream frame or an equivalent construction mechanism to produce consistent pairs. Normal update/save/replay paths must construct both together. Do not make arbitrary replacement of `State` or `Stream` a public business operation.

Opening construction starts with the supplied immutable opening state and a version-zero stream. Historical construction must go through replay or validated snapshot restoration. A subclass rewrapping a saved view must preserve the matching state, rule binding, and exact stream prefix.

The generic framework still trusts correctly implemented handlers and accepted snapshot business data. A version check alone cannot prove that an arbitrary supplied state matches history; unsafe/manual restoration boundaries must be explicitly documented and kept separate from normal construction.

### 6.2 Forward application

An aggregate application operation equivalent to `Apply(IEvent)` must:

1. Validate event acceptance and obtain the correctly tagged clone through the stream machinery.
2. Resolve the exact handler and compute the new state using that accepted event.
3. Reject null state results and propagate business validation failures.
4. Publish a new consistent aggregate view only after both event acceptance and reduction succeed.

The input aggregate, its state, its entities, and its stream remain unchanged on success or failure. An unknown event must not enter the aggregate's published pending history.

Business roots can expose methods such as `ReceiveIncome`, `Transfer`, or `ProcessMonth`, wrapping this application path. Reducers/handlers must enforce invariants even when callers use the generic application operation.

A batch application operation must return a complete new root or fail without publishing a partial result. It may use local immutable intermediate views; it must not mutate the input root.

### 6.3 Saving

Aggregate save persists its stream's pending batch and returns a new root with unchanged business state and the returned committed stream view. No reducer, business callback, or fallible user-supplied root-construction work should run after durable commit in a way that reports the committed operation as failed.

Design the wrapping mechanism so publication is prepared before I/O or the post-commit saved-view construction is framework-controlled. Preserve the existing contract that successful durable commit is reported as success.

`HandleAsync` may combine aggregate application and save. Storage failure must not publish a changed root as a successful durable result. Callers retain the returned saved root; reuse of an older successfully superseded branch causes an expected-version conflict.

Expose `RuleVersion` from the bound dispatcher/reducer at aggregate level. Delegate stream lifecycle properties without independently writable duplicate counters.

## 7. Typed handlers and dispatcher

Retain `IEventHandler<TState, TEvent>` with pure `Do` and `Undo`; constrain new registrations to concrete classes satisfying `IEvent` without requiring a shared application event hierarchy.

Refactor dispatcher and builder to `EventHandlerDispatcher<TState>` and `EventHandlerDispatcherBuilder<TState>`. Their public input is `IEvent`; their registration method remains generic over the concrete event:

```csharp
var dispatcher = EventHandlerDispatcher<HouseholdState>
    .CreateBuilder("household/v2")
    .Register<IncomeReceived>(incomeHandler)
    .Register<MoneyTransferred>(transferHandler)
    .Register<MonthClosed>(monthCloseHandler)
    .Build();
```

This illustrates the target API. Test the actual implementation from an external assembly and generated NuGet package.

Internally, registration can create typed delegate adapters and store them behind a common `IEvent` entry. Exact runtime-type selection ensures the registered cast is valid. Retain strongly typed handler implementations; do not require reflection invocation or `dynamic` for each event transition.

Preserve duplicate registration rejection, missing-handler errors with `EventType`, null-input/result rejection, immutable registration maps, and the builder's startup-only non-thread-safe contract. A built dispatcher is unaffected by later builder changes.

Handlers use the supplied state/event, not captured root state. Retained instances and dependencies must remain pure, reentrant, and alive for their dispatcher lifetime.

If a reducer interface is retained, refactor it to `IEventReducer<TState>` with identified pure reduction from `(TState, IEvent)` to `TState`. It belongs to aggregate application/replay rather than stream storage.

## 8. Undo and compensation

Retain the immediate reversal invariant for valid inputs:

```text
Undo(Do(state, event), event) == state
```

Events carry all required reversal information. Handlers validate reversal preconditions and return a complete restored business state without mutation, I/O, clocks, or hidden history lookups.

The dispatcher may expose pure `Undo(TState, IEvent)`. It computes a state value only. It must not construct a normal root using that reversed state alongside unchanged event history, remove committed events, or decrement versions.

Persisted business cancellation uses a new compensating event and retains the original history. A general historical rollback/reopening engine is outside this refactor.

## 9. Replay and rule provenance

Move state replay/load orchestration from `EventStream<TState, TEvent>` to the aggregate layer or a dedicated typed aggregate-rehydration component. The public stream remains state-free.

Retain `ReplayDefinition<TState>` as original opening state plus transition `RuleVersion`. Applications persist definitions consistently with event histories; event stores continue storing events rather than silently becoming definition repositories.

Full replay validates event identity, metadata, contiguous stream membership, duplicates, and recorded rule compatibility before publishing a root. It reduces accepted events in version order using the same bound mechanism as live application. The resulting root is committed, with no pending events.

Replay must not invoke allocation policies, commands, persistence callbacks, external side effects, ambient services, or ID generation. Unknown concrete event kinds fail explicitly.

Rule identity includes transition implementation and immutable configuration. Retain historical implementations and select them explicitly. Moving `RuleVersion` off the stream must not weaken mismatch rejection or allow arbitrary snapshot relabeling.

Do not automatically interpret an old scenario with newly selected rules. Schema upgrades, rule migrations, and deliberate scenario branching remain application/adapter responsibilities.

## 10. Aggregate snapshots

Keep `AggregateSnapshot<TState>` and `ISnapshotStore<TState>` state-typed. Snapshots contain complete root/entity business data, aggregate identity, committed stream version, transition rule identity, and positive state schema version.

Capture and snapshot save move to/remain on the aggregate layer and derive provenance from its bound rules. Capture rejects pending changes, including changes whose event save failed.

Restore by selecting a compatible snapshot, reading the state-free stream suffix after its committed cursor, and reducing only later events from the snapshot state. Reconstruct a consistent root and stream pair. Restore the prefix identity index for duplicate rejection.

No compatible snapshot means full replay from the original opening definition. Corruption and invalid suffixes fail explicitly. A checkpoint ahead of the event ledger, wrong aggregate identity, mismatched rules/schema, incomplete suffix, or duplicate IDs must be rejected.

Snapshot saving remains separate from event commit; checkpoint failure cannot undo a committed event batch. Preserve full event history and opening definitions.

The stream's `HistoryStartVersion` marks the checkpoint boundary, but the stream has no state checkpoint object. Do not infer the full version from the retained suffix length.

Snapshot acceleration does not imply constant-time loading: prior event IDs are still required, and a domain state containing all statements grows with its retained business data. Preserve and benchmark this existing correctness tradeoff.

## 11. Non-generic stores and multi-aggregate atomicity

Refactor the core event ports and event-history DTOs to heterogeneous non-generic forms:

- `IEventStreamStore` with reads/appends of `IEvent`.
- `IAtomicEventStreamStore` with atomic batch appends and consistent batch reads.
- `StreamSlice`, `StreamAppend`, event-history containers, and in-memory stores without a required `TEvent`.
- `StreamReadRequest` retaining stream identity and cursor semantics.

Retain whole-batch expected-version checking, duplicate validation, metadata validation, immutable returned views, thread-safe in-memory publication, and cancellation behavior. Independent sequential saves cannot implement atomic batch persistence.

Separate aggregates can have different state types and event sets in one event-store transaction. A batch helper may save their prepared streams without knowing business state types. Applications then retain each matching typed aggregate state and returned committed stream. The helper must provide a safe pairing/wrapping contract and must not use unsafe casts to pretend heterogeneous state types are identical.

Unchanged participants can assert expected versions as read dependencies. Duplicate stream participants fail. Consistent `ReadBatchFromAsync` remains required for multi-root restoration; separate reads can observe different commits.

Duplicate event GUID scope remains per logical stream. Correlated operations in separate streams may use shared source identities as allowed by the existing contract. A stream registry must not silently convert this into a global duplicate rule.

## 12. Serialization and compatibility

Non-generic event containers require explicit polymorphic serialization in durable adapters. Use stable domain event discriminators and payload schema versions; do not rely on CLR assembly-qualified names or uncontrolled runtime type activation.

Deserialization yields concrete events satisfying `IEvent`. Unknown kinds, unsupported payload versions, invalid metadata, and corrupt histories fail before returning a reconstructed aggregate.

API incompatibility and data-format incompatibility are distinct. Preserve existing persisted discriminator/payload formats where possible. When formats change, provide a documented adapter migration or explicit rejection; do not assume a C# API refactor changes historical event meaning.

Snapshots remain independently versioned. Source migration must not relabel state schema or transition rule identity without a real corresponding change.

## 13. Optional dependency injection package

Update `EventSourcing.DependencyInjection` to the new state-only dispatcher API. Registration and resolution are keyed by the state type and explicit transition rule version. Concrete handler registration retains its event type parameter.

External libraries must still expose a single registration entry point, use constructor injection, reject duplicate event mappings/rule registrations, select historical dispatchers explicitly, and support manual composition without a container.

Preserve retained instances, scope lifetime validation/disposal behavior, and no container lookups during `Do`/`Undo`. Do not add assembly scanning as an incidental consequence of this refactor.

Publish the adapter against the matching core version and validate the generated packages together. Core must not acquire a DI-framework dependency.

## 14. Breaking-change migration

Document at least these substitutions:

| Existing API | Target API / responsibility |
| --- | --- |
| `EventSourcedAggregate<TState, TEvent>` | `EventSourcedAggregate<TState>` with paired state/stream construction. |
| `IEventSourcedAggregate<TState, TEvent>` | `IEventSourcedAggregate<TState>`. |
| `EventStream<TState, TEvent>` | State-free heterogeneous `EventStream`. |
| Stream `Apply` / state replay | Aggregate application and aggregate rehydration. |
| `EventHandlerDispatcher<TState, TEvent>` | `EventHandlerDispatcher<TState>`. |
| `IEventReducer<TState, TEvent>` | State-only reducer consuming `IEvent`, if this abstraction remains. |
| Required `IEvent<TEvent>` event hierarchy | Non-generic `IEvent`; optional typed bridge. |
| `IEventStreamStore<TEvent>` and related generic DTOs | Non-generic heterogeneous ports and DTOs. |
| Stream state/rule/snapshot access | Aggregate state/rule/snapshot access. |
| Existing DI registration/resolution signatures | State-only dispatcher composition with typed handler registration. |

Existing business roots must update constructors, application wrappers, save wrappers, and replay/load calls. Examples must demonstrate the authoritative state moving to the aggregate without reintroducing independent copies.

The legacy `EventSourcing.Compatibility` package must continue to build against the matching core or have an explicit separately documented migration. Do not silently redefine its reversible callback lifecycle or direct new applications toward it.

Avoid maintaining two competing core architectures indefinitely. Transitional wrappers, if provided, must forward to one implementation and document obsolete APIs and removal policy. Backward source compatibility is not an acceptance requirement for this explicitly breaking refactor; preservation of promised persistence/lifecycle guarantees is.

Use a new package version and release tag. A proposed next release is `0.2.0`, subject to the repository's chosen versioning policy. Never overwrite an existing published package version.

## 15. Acceptance tests

### 15.1 Public contracts and external consumers

- A consumer declares unrelated concrete events implementing `IEvent` without a shared application base class.
- A business root derives from `EventSourcedAggregate<ConsumerState>` and handles those events through typed handlers.
- A standalone `EventStream` requires no state object, reducer, or state type parameter.
- Public store, batch, slice, and DI APIs require no common event type parameter.
- A generated-package consumer uses only NuGet references and public APIs, without project references to source internals.

### 15.2 Heterogeneous ordering and financial behavior

- Apply an income, transfer, expense, and closure in an interleaved sequence; assert exact event types, IDs, positions, and final business results.
- Full replay preserves the same order and state. Different event dates do not reorder history.
- Transfer updates both account entities once and conserves total assets; expense counts once and statements reconcile.
- Missing handlers, handler exceptions, null results, clone failures, duplicates, and failed batch application preserve the original complete state/stream pair.
- Earlier branches and returned collections remain deeply immutable.

### 15.3 Reversal, rules, and snapshots

- Each typed handler satisfies complete-state immediate reversal and rejects invalid reversal preconditions.
- Pure Undo leaves stream history untouched; persisted compensation appends a new correlated event.
- Wrong rule identity fails before reduction. Built dispatchers retain frozen mappings/configuration.
- Complete entity state survives a serialized committed snapshot and suffix replay.
- Snapshot restoration equals full replay and supports equivalent subsequent application/saving.
- Pending snapshot capture fails; checkpoint-write failure preserves committed history.
- Duplicates preceding the checkpoint and invalid cursors/suffixes are rejected.

### 15.4 Persistence and concurrency integration

Save, reopen, deserialize, and replay heterogeneous events through a real file-backed test adapter with stable discriminators. Include original opening definitions and rule provenance.

Test competing expected versions, atomic multi-root writes with different state types, consistent batch reads/restoration, unchanged read dependencies, duplicate stream participants, cancellation, corrupted formats, and failed full-batch writes preserving durable data.

Verify that successful durable commit is not subsequently reported as failure because of a user reducer or root-wrapper callback. Verify the application continues with the returned committed root.

### 15.5 DI and compatibility regression

Test external handler constructor injection, client-library registration, keyed historical rules, duplicate rejection, retained handler instances for Do/Undo, scoped disposal, and manual composition equivalence.

Run the existing compatibility test suite or the explicitly updated equivalent. Existing reversal, metadata, concurrency, and snapshot guarantees must not disappear during generic-parameter removal.

## 16. BenchmarkDotNet performance coverage

Keep a dedicated BenchmarkDotNet performance project separate from correctness tests and core dependencies. Update existing benchmarks to the new public model.

Compare equivalent workloads before/after the refactor: homogeneous versus interleaved heterogeneous dispatch, append and aggregate apply, saving each event versus batches, full replay, snapshot-plus-suffix restore, snapshot serialization, and atomic multi-aggregate saves.

Record time and allocations across increasing event counts. Check for introduced boxing, per-event reflection, duplicate state copies, or loss of structural sharing. Prepare event IDs and fixtures outside timing where appropriate.

Check resulting state, versions, event counts, and snapshot/entity equivalence outside measured operations. Preserve machine/runtime and baseline provenance. Ordinary CI must not depend on arbitrary elapsed-time assertions.

## 17. Implementation sequence and release gate

Implement the following sequence using focused failing acceptance tests before each behavior change:

1. Establish the non-generic event/clone contract and its optional typed bridge.
2. Introduce the state-free stream and heterogeneous storage DTOs/ports while retaining history and concurrency guarantees.
3. Refactor typed dispatcher/reducer composition to state-only public signatures.
4. Move authoritative state and forward/batch application to the aggregate; enforce consistent paired construction and safe save publication.
5. Move full replay, guarded definitions, snapshot capture, and suffix restoration to the aggregate layer.
6. Update atomic batch helpers, durable integration fixtures, and unrelated-event external consumer tests.
7. Update DI and compatibility packages, package-only consumers, examples, XML documentation, and migration guidance.
8. Run full Release correctness checks and record relevant BenchmarkDotNet results.
9. Build/pack a new version and validate consuming the actual generated packages before publishing.
10. Publish the matching package versions and release notes, then update Family Horizon to consume them.

Use the repository's required restore/build/test commands and warning checks. Update release automation only where the new package structure/contracts require it. Publishing is a later explicit operation after implementation and validation; this specification does not publish packages or alter repository files.

The release is complete only when all affected packages build, the regression/integration suites pass, package consumers compile and run, migration docs match the implemented public API, and no lifecycle guarantee has been lost.

## 18. Consequences for Family Horizon

After the refactored package is published, update the earlier FinancialState specification to use:

```csharp
FinancialState : EventSourcedAggregate<FinancialStateData>
```

`FinancialStateData` remains the complete deeply immutable root/entity state. Accounts, account monthly statements, consolidated monthly statements, allocation policy behavior, financial invariants, and complete snapshots keep their previously specified meaning.

The root owns its state and composes a heterogeneous `EventStream`; its dispatcher is `EventHandlerDispatcher<FinancialStateData>`. A common `FinancialEvent` base can remain a domain convenience but is no longer required by the library.

State replay/load and rule-bound snapshots use the aggregate layer. Update lifecycle examples and integration tests accordingly. The existing FinancialState document describes the pre-refactor library contract until that follow-up revision; this specification governs the library work that comes first.

## 19. Verified baseline references

Contracts inspected at EventSourcing repository commit `82317e10f30e5b26744e0aacc2a87a0b3556323b`:

- [Current event and clone contracts](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Events/IEvent.cs)
- [Current business aggregate](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/ObjectsAggregation/EventSourcedAggregate.cs)
- [Current typed dispatcher](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Handlers/EventHandlerDispatcher.cs)
- [Single-stream persistence port](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Streams/IEventStreamStore.cs)
- [Atomic storage port](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Streams/IAtomicEventStreamStore.cs)
- [Replay definition](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Streams/ReplayDefinition.cs)
- [Aggregate snapshot](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/src/EventSourcing/Snapshots/AggregateSnapshot.cs)
- [Lifecycle and reversal requirements](https://github.com/GaelOn/EventSourcing/blob/82317e10f30e5b26744e0aacc2a87a0b3556323b/docs/refinement-lifecycle.md)
