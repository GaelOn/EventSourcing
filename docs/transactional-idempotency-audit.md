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
