# Transactional operation processing — 0.3.0

`EventSourcing.Transactions` adds an injectable **single-store** operation boundary.
`OperationProcessor` calls `IEventTransactionCoordinator`; the default implementation
is `LocalEventTransactionCoordinator`. `OperationReceipt` is coordinator independent.
Alternative implementations can construct ownership handles and return receipts;
they must uphold the same guarantees. No distributed participant/prepare protocol
is prescribed. Future coordinators may need additional participant contracts.

## Live admission

Supply an opaque `OperationKey(namespace, value)` for one intended operation.
Namespace should contain application, tenant/ledger and key-algorithm version.
Ordinal case-sensitive strings are retained verbatim, not trimmed or hashed.
The client owns collision prevention and stable semantic identity. Changing event
IDs on redelivery does not change the key. Equal payloads with distinct keys remain
legitimate distinct operations. `contractVersion` is immutable metadata: reusing a
key under another contract fails with `OperationIntegrityException`. No reflection,
structural payload equality, runtime hash, random retry ID or fingerprint establishes
semantic identity. Keys are bounded to 512/2048 characters; whitespace-only keys fail.

Reserve one stable caller-configured **journal stream ID** per ledger and use it for
all coordinators. It is separate from domain streams, operation keys and attempt IDs.
Never change it on restart, use different journals for the same identity scope, write
business events to it or bypass coordination for an operation claimed to be idempotent.
IDs and immutable domain events remain caller supplied. Library-generated GUIDs identify
only transaction attempts and journal markers, never business semantic identity.

```csharp
var coordinator = new LocalEventTransactionCoordinator(store, retainedJournalId);
await coordinator.RecoverAsync(); // Before admitting work after reopening storage.
var processor = new OperationProcessor(coordinator);
var result = await processor.ExecuteAsync(
    new OperationKey("synthetic-ledger/tenant-1/v1", "transfer-1"), "transfer/v1",
    async cancellation =>
    {
        var history = await store.ReadBatchAsync(new[] { sourceId, destinationId }, cancellation);
        var source = EventStream.Restore(sourceId, history[sourceId])
            .Append(new SyntheticPosting(sourceEventId, -25m, "source"));
        var destination = EventStream.Restore(destinationId, history[destinationId])
            .Append(new SyntheticPosting(destinationEventId, 25m, "destination"));
        return new OperationPlan(new[] { source, destination }.Select(stream =>
            new StreamAppend(stream.Id, stream.PersistedVersion, stream.PendingEvents)), "transferred");
    }, wait: TimeSpan.FromSeconds(5));
```

[The executable package consumer](../validation/TransactionConsumer/Program.cs)
contains complete artificial event definitions and a compiling end-to-end example.
`OperationPlan` contains isolated immutable branches, all affected streams and unchanged
read dependencies. It may include authoritative cursor/progress events. The callback
must not mutate shared projections or perform external effects. No generic coordinator
can roll back arbitrary caller side effects. Publish a new in-memory aggregate/view
only after success, or reload from committed events. State is derived from that history;
there is no second independently persisted business-state authority.

Claim markers are durable but expose no committed business effects. Staging is disposable
in-memory data. Commit revalidates the owned attempt, journal and all stream versions,
then atomically publishes the complete domain batch and consumed receipt in that store.
A no-op plan still commits a consumed receipt and retained outcome. Receipt ranges
identify every ordered event ID and original expected version, without altering payloads.
The outcome is client-defined versioned data; avoid secrets or unnecessary personal facts.

`Committed` reports this invocation's publication. `AlreadyConsumed` is an expected
result with the original receipt: no callback, domain append or recursive duplicate event.
`InProgress` means an owner remains unresolved; timeout never permits another execution.
`ResolveAsync` waits cancellably for up to five minutes, returning consumed, in-progress
or retry-allowed. The processor waits only when asked, and retries admission only after
completed rollback. Cancellation of a waiter does not cancel the owning transaction.
Conflicts remain explicit; no automatic business re-evaluation or catch-all retry loop.

Exceptions before publication cause rollback, leaving no domain events or consumed
receipt. Failed rollback remains explicit/unresolved; stop execution and complete
rollback through the ownership handle or reopen/recover the store. A successful commit
followed by response loss remains committed. Coordinator rereads evidence after ambiguous
claim, rollback and commit acknowledgements. Cancellation after publication never retracts
history. Callers retry with the original key and immutable metadata after uncertain responses.

## Durable local adapter and recovery

Core has no filesystem dependency. Optional `EventSourcing.Storage.Json` provides
`JsonEventStreamStore` with an explicit frozen `JsonEventCodec` discriminator registry.
The reserved `$operation-journal/v1` mapping handles journal records; register immutable
application event types under stable versioned discriminator strings. Unsupported types,
formats, malformed metadata, inconsistent lifecycle markers and receipt/history mismatches
fail before publishing/restoring data. The adapter never logs event payloads or secrets.

One store instance/process holds an exclusive OS file lease for its lifetime. Thread-safe
batch reads/writes share a gate. Canonical directory aliases resolve to the same lease;
symbolic-link documents/lease files are rejected. Use one configured path on a local
filesystem, without hard-link aliases, external file writers or manual file replacement.
Different store instances are not simultaneously supported. `ITransactionSessionStore`
requires a stable lifetime session ID and guarantees that older sessions cannot execute
when recovery is authorized. In-memory storage is explicitly nondurable.

Whole documents contain full immutable histories plus journal evidence. Publication
writes a private same-directory staging file, flushes it to disk and atomically replaces
the committed document. Only then does the adapter publish its in-memory view. On reopening,
it validates the committed document before cleaning orphan staging files. Recovery appends
rollback markers for abandoned older-session claims, preserving claims and committed history;
repeated recovery is a no-op. Old disposed stores and recovered attempts cannot commit.
Current-session claims are never stolen based on elapsed time. Partial domain batches
are never committed. This is **process-crash recovery**, not a power-loss guarantee,
network-filesystem safety, distributed 2PC or atomic external-effect delivery.

Fault observers expose before/after replacement barriers for diagnostics/tests; they must
not reenter the store. After-replacement exceptions mean acknowledgement loss, not rollback.
The test host is forcibly terminated at staging, before replacement and after replacement
without normal cleanup, and checks exact retained payloads/ranges across repeated reopen.

## Replay, prefix selection and snapshots

Historical reduction and proposal admission are separate. Apply every recorded domain
event once in sequence under retained rules. Never skip that event because its operation
was consumed. `ReplayOperationAdmission` is an immutable view constructed from one
consistent **selected full-prefix** history including journal and participant streams.
Its receipt index may include recorded consequences later within that selected prefix,
without applying them early. Re-emitted proposals resolve `AlreadyConsumed`; the original
consequence is still reduced at its historical position. Absent proposals return
`ReplaySuppressed`, never execute a live handler or append evidence. Metadata conflicts
and inconsistent/truncated receipt ranges fail explicitly. Unrelated later/live receipts
must never be provided to a historical prefix view.

Restore a business snapshot with its validated suffix using existing APIs. Reconstruct
identity evidence from the **selected retained full journal and participant history**,
not merely the snapshot suffix or current unrelated ledger. Receipts remain available
for operations before the checkpoint. A snapshot does not delete full history or receipts.
Replay neither writes the journal nor changes original facts/order/reasons. Continuation
switches explicitly back to live `OperationProcessor` admission after reconstruction.

## DI and extensions

Register one `ITransactionSessionStore` lifetime, then call
`services.AddLocalEventTransactions(retainedJournalId)` from the optional DI package.
This registers the coordinator and `OperationProcessor` as singletons. Duplicate coordinator
registration fails. Standard DI replacement can substitute another coordinator without
changing processing logic; its storage/ownership/recovery implementation must satisfy the
same atomic publication and isolation contract. Core/manual composition stays dependency free.

## Compatibility exceptions and version boundaries

| Supported contract / consumers | Boundary and tests | Retirement condition |
| --- | --- | --- |
| Published 0.2.0 `IEvent`, stream, aggregate, dispatcher, snapshot and low-level store clients; three existing package consumers | Their public signatures and existing payload/metadata wire meaning are unchanged; original correctness suites and package consumers verify them. They provide event-ID atomic persistence, **not semantic processing idempotency**. New processing must use the keyed coordinator. | Remove deprecated entry points only in a separately reviewed breaking release after consumers migrate; no historical keys are fabricated. |
| Optional published callback compatibility package consumers | Its preexisting lifecycle remains in the separate package; compatibility tests/sample remain green. It does not acquire durable idempotency by upgrading. | A future explicitly approved breaking package release after known callback consumers migrate. |
| Existing serialized client event histories without operation receipts | Still replay using their registered codecs/rules. New JSON format 1 is separate from old test fixture formats and rejects those files explicitly. Empty journal means replay proposals are suppressed, not assigned invented identities. | A consuming application's explicitly reviewed import mapping may establish deterministic operation identities; only then may it assert historical operation deduplication. No automatic migration. |

Four matching **0.3.0** packages are generated locally: core, DI, compatibility and JSON.
No existing immutable registry version is overwritten. Family Horizon remains unchanged;
its integration is a separate iteration. It must adapt its actual atomic JSON boundary
and pinned history, not silently replace it with this demonstration's new store format.
Use the tested local `.nupkg` files or an explicitly authorized project-reference trial
before publication. Clean remote CI cannot restore this unpublished version until the
release is published or exact locally built package artifacts are supplied.

See [release handoff](release-0.3.0.md) for validation, availability and publication commands.
