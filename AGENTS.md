# EventSourcing contributor instructions

- Use .NET 10, C#, nullable reference types, `.slnx`, and xUnit.
- Keep this repository independently buildable; do not reference files or projects outside it.
- Prefer immutable state and events, explicit caller-supplied IDs, deterministic reducers, and atomic expected-version checks.
- `EventSourcing` contains supported business aggregate roots, immutable streams, snapshot checkpoints, and storage contracts. The optional `EventSourcing.Compatibility` package owns the legacy `CallbackEventSourcedAggregate` lifecycle and depends on core; core must not depend on compatibility.
- Events expose `EventMetadata` through `Metadata`; stream identity is `StreamId`. Event identity remains separate from stream position.
- Applications persist immutable opening inputs and rule versions alongside stream history. Prefer the `ReplayDefinition` overload for persisted scenarios and retain the matching reducer; event-only storage cannot infer replay provenance.
- Preserve structural sharing of history and per-stream identity indexes. The dependency-free benchmark under `benchmarks/` measures aggregate apply and apply-and-save costs; timings are diagnostics, not test thresholds.
- Add behavior-focused failing tests before changing behavior. Never weaken a valid test to obtain a green build.
- Preserve the all-or-nothing batch contract and consistent multi-stream reads. Add reconciliation and concurrency tests when changing those guarantees.
- Run `dotnet restore EventSourcing.slnx`, `dotnet build EventSourcing.slnx --no-restore`, and `dotnet test EventSourcing.slnx --no-build` before completion.
- Keep generated `bin`, `obj`, test output, packages, and secrets out of Git. Review the staged diff before committing.
- Document public API changes in the README. Keep the core library independent of filesystem and application-specific dependencies.

- The core `EventSourcedAggregate` owns the authoritative immutable business state and projects entities from it; its state-free heterogeneous stream contains history/lifecycle only. Do not cache independent state/entity copies in derived roots.
- Snapshot cursors are persisted stream versions, never event GUIDs or retained-history counts. `Events` starts after `HistoryStartVersion`; restored `Version` remains the complete stream position.
- Capture checkpoints only without pending events. Save them separately after event commits; failed checkpoint writes never undo committed events. Keep full events and opening definitions for rebuilding.
- Snapshot compatibility uses aggregate ID, rule version, and state schema version. Load the newest compatible checkpoint, replay only its validated suffix, and retain earlier IDs for immediate duplicate rejection. Propagate corruption/storage errors and reject ahead-of-ledger checkpoints.
- Restore related roots from one consistent `ReadBatchFromAsync` result. Individual snapshot loads do not provide a consistent household view. Retain reconciliation, stale-write, failure, cancellation, and concurrency tests for snapshots and range reads.

- Typed state handlers are pure, reentrant transitions selected by exact runtime event type. Freeze dispatcher registrations, reject duplicates/missing handlers, and include handler configuration in the replay rule version. Core does not depend on a DI framework or implement command dispatch.

- Handlers provide pure Do/Undo transitions and enforce reversal preconditions. Immediate reversal restores the input state. Persisted cancellation appends a new compensating event; never remove history.
- Prefer IEventReducer/identified dispatchers and automatic snapshot identity. Preserve bound rules through aggregate application/replay/snapshot lifecycle operations. Public Stream.Append is a state-free infrastructure operation; application façades restrict business entry points when needed.
- BenchmarkDotNet lives only in EventSourcing.PerformanceTests. Setup/IDs and correctness assertions stay outside measured operations; benchmarks run separately from correctness CI without timing thresholds.

- `EventSourcing.DependencyInjection` is an optional package referencing core and Microsoft.Extensions.DependencyInjection.Abstractions only. Core never references the adapter or a DI framework; preserve manual composition.
- Register explicitly identified keyed dispatchers, resolve/retain handlers only during construction, reject duplicate event/rule registrations, and freeze mapping configuration. Enforce scope lifecycle for further transitions; enable real-container scope validation in integration tests.
- DI handler fixtures separate a core-only business assembly from the optional client registration module. Container implementation dependencies belong only to tests and consuming applications. Keep package-only consumers outside the solution until packages are generated.

- Transactional processing requires a stable scoped client-defined `OperationKey`, immutable contract version, one retained reserved journal stream and an `IEventTransactionCoordinator`. Event IDs and expected-version atomicity alone do not provide semantic idempotency. No keys are inferred for old history.
- Stage isolated immutable consequences; commit the entire domain batch, authoritative progress and consumed receipt in the same store. No-op decisions also retain receipts. Duplicate/time-out/replay-suppressed outcomes must not execute consequences or emit recursive operations.
- Replay reduces every original event in order; proposal admission uses only the consistent selected full-prefix journal/history. Reconstruct pre-snapshot identity evidence from retained history. Never consult unrelated later/live receipts.
- Optional `EventSourcing.Storage.Json` owns local filesystem persistence, stable explicit event codecs, atomic whole-document replacement and a lifetime exclusive OS lease. Core has no filesystem dependency. Recover abandoned older-session claims before admitting work; do not steal current claims on timeout. Validate corrupt evidence and preserve committed history. Process-crash recovery is tested; distributed/external-effect, power-loss and network-filesystem guarantees are outside scope.
- Keep forced-process crash tests at explicit barriers, plus concurrency, rollback, lost-response, cancellation, exact range/payload and persisted snapshot/replay tests green. Release builds generate matching 0.3.0 packages; validate all four package-only consumers with an isolated cache. Publishing remains an explicit external action. See docs/transactional-idempotency.md.
