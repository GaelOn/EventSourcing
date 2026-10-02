# EventSourcing 0.2.0 release notes

Business aggregates now own the authoritative immutable state and its transition rules. A state-free,
non-generic EventStream records all unrelated concrete events in one ordered version sequence. Streams,
stores, append/slice DTOs and atomic operations require no application common-event type. Typed handlers
remain compile-time typed; dispatchers and optional DI registration now have only the state parameter.

This is a breaking source change. Follow [migration-0.2.0.md](migration-0.2.0.md) for constructors,
application/save wrappers, replay, snapshots, event clone bridges, and DI signatures. Existing stable
adapter discriminator/payload formats retain their meaning. Core remains dependency-free. The separate
DI and compatibility packages target the matching 0.2.0 core; compatibility callbacks keep their lifecycle.

Forward application publishes a complete immutable state/history frame. Runtime business-root type is
preserved by framework-controlled copies; saved roots and atomic multi-root results are prepared before
I/O so no user reducer, constructor or root wrapper runs after durable commit. Pure Undo returns state
only. Persisted cancellation appends compensation and retains original history.

Validation includes Release with warnings treated as errors, 181 passing correctness tests, all 16
benchmark workload configurations checked outside timing, and three isolated generated-NuGet consumers
(core/manual, DI, compatibility). The core-only consumer has no transitive DI dependency. XML documents,
MIT metadata, matching versions/dependencies and package contents are checked. Performance reports and
limitations are recorded separately under benchmarks/EventSourcing.PerformanceTests/baselines/.

These notes prepare a later explicit release. No package publication, tag creation or Family Horizon
package update is performed by this implementation. After publication, update Family Horizon and its
FinancialState design to the state-owning aggregate API as a separate follow-up.
