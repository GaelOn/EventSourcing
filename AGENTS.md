# EventSourcing contributor instructions

- Use .NET 10, C#, nullable reference types, `.slnx`, and xUnit.
- Keep this repository independently buildable; do not reference files or projects outside it.
- Prefer immutable state and events, explicit caller-supplied IDs, deterministic reducers, and atomic expected-version checks.
- `EventSourcing` contains supported business aggregate roots, immutable streams, snapshot checkpoints, and storage contracts. The optional `EventSourcing.Compatibility` package owns the legacy `CallbackEventSourcedAggregate` lifecycle and depends on core; core must not depend on compatibility.
- Events expose `EventMetadata` through `Metadata`; stream identity is `StreamId`. Event identity remains separate from stream position.
- Applications persist immutable opening inputs and rule versions alongside stream history. Prefer the `ReplayDefinition` overload for persisted scenarios and retain the matching reducer; event-only storage cannot infer replay provenance.
- Preserve structural sharing of history and per-stream identity indexes. The dependency-free benchmark under `benchmarks/` measures apply and apply-and-save costs; timings are diagnostics, not test thresholds.
- Add behavior-focused failing tests before changing behavior. Never weaken a valid test to obtain a green build.
- Preserve the all-or-nothing batch contract and consistent multi-stream reads. Add reconciliation and concurrency tests when changing those guarantees.
- Run `dotnet restore EventSourcing.slnx`, `dotnet build EventSourcing.slnx --no-restore`, and `dotnet test EventSourcing.slnx --no-build` before completion.
- Keep generated `bin`, `obj`, test output, packages, and secrets out of Git. Review the staged diff before committing.
- Document public API changes in the README. Keep the core library independent of filesystem and application-specific dependencies.

- The core `EventSourcedAggregate` composes a stream and projects its complete immutable business state/entities; it is not obsolete. Do not maintain a second state copy in the root.
- Snapshot cursors are persisted stream versions, never event GUIDs or retained-history counts. `Events` starts after `HistoryStartVersion`; restored `Version` remains the complete stream position.
- Capture checkpoints only without pending events. Save them separately after event commits; failed checkpoint writes never undo committed events. Keep full events and opening definitions for rebuilding.
- Snapshot compatibility uses aggregate ID, rule version, and state schema version. Load the newest compatible checkpoint, replay only its validated suffix, and retain earlier IDs for immediate duplicate rejection. Propagate corruption/storage errors and reject ahead-of-ledger checkpoints.
- Restore related roots from one consistent `ReadBatchFromAsync` result. Individual snapshot loads do not provide a consistent household view. Retain reconciliation, stale-write, failure, cancellation, and concurrency tests for snapshots and range reads.
