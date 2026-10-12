# Transactional idempotency audit — M0

Baseline: 9202bc6, .NET 10.0.400. Existing `EventStream.Append/SaveAsync`,
`EventStreamBatch.SaveAsync` and `IAtomicEventStreamStore.AppendBatchAsync` enforce
per-stream event IDs, metadata and expected versions. Immutable aggregates isolate
failed branches. The compatibility callback API has Do/Undo, but does not prove
crash durability. Dispatchers are pure; normal aggregate replay does not append.
The synthetic receiver demonstrates the missing admission boundary when clients
submit consequences from a replay rule; it does not allege that pure replay emits.

Executed the compiling `TransactionalIdempotencyRegressionTests` before changing
implementation: 2 failed, 2 passed. `Same_operation_with_new_event_ID_has_one_committed_effect`
found 2 events instead of 1. `Replay_proposed_threshold_transfer_does_not_duplicate_recorded_consequence`
found 3 events instead of 2. Equal payloads with different identities were accepted;
repeated event IDs were correctly rejected. Log: /private/tmp/es-idem-m0-red.log.

No semantic key, consumed receipt, claim/rollback owner fencing or injectable
processing coordinator exists. JSON persistence is a synthetic test fixture,
not a supported durable arbitrary-event adapter. In-memory atomicity alone is
not durable idempotency. A new optional local JSON adapter must keep filesystem
concerns out of core. Its scope will be one exclusively leased process per store,
thread-safe within that process, with whole-document atomic replacement. No
external effects, distributed coordination or power-loss durability is inferred.
Historical APIs retain their published low-level persistence meaning; no keys
will be invented for old events. New processing explicitly requires operation keys.

## M1–M3 correction and Green evidence

M1 introduced isolated plans and atomic claim/receipt publication; 28 focused
boundary/multi-stream tests passed. M2 added scoped semantic identities,
coordinator-independent outcomes, bounded duplicate resolution and historical
proposal suppression; all 12 initial targeted cases passed, including both M0
failures. M3 executes those regressions and concurrent/no-op/rollback admission
against both the in-memory store and the production `EventSourcing.Storage.Json`
adapter. Matching opaque keys never require structural payload comparisons.

Production JSON tests launch a real child .NET process, block at explicit barriers,
and forcibly terminate it without cleanup: staged, before atomic replacement,
and after replacement. A second process/instance cannot open the leased store.
Reopening validates exact history and receipts, rolls back only abandoned
older-session claims, and retries the same key. Repeated recovery/reload has
exactly one source/destination event, original IDs, amounts, reasons and positions,
and one consumed receipt. Tests also cover lost claim/rollback/commit responses,
before/after-commit cancellation, stale/disposed owners, no-op restart,
fresh delivery IDs, persisted snapshots, repeated replay, corrupt evidence,
unsupported formats and canonical directory alias lease sharing.

A review initially rejected standard macOS `/var` aliases too broadly; the
corrected adapter resolves ancestor aliases before acquiring the lease. The
same full tests then passed. No failing assertions were removed or weakened.
Final M3 commands: restore solution; Debug build with `-warnaserror`; full tests:
200 core + 21 DI passed, zero warnings/errors. Logs:
/private/tmp/es-idem-m3-reviewed-build.log and
/private/tmp/es-idem-m3-reviewed-test.log.

Publication flushes a private same-directory staging file and atomically replaces
one complete document. Durable claims are separate from committed business
outcomes. In-memory plans are disposable; accepted outcomes and processing
progress must be represented by the same event batch/receipt. File ownership is
exclusive for the store lifetime; a new session can recover only when the old
store lease is gone. This proves process-crash recovery, not power-loss,
network-filesystem, distributed transactions or external-effect delivery.
