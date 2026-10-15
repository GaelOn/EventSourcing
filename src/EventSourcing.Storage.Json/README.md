# EventSourcing.Storage.Json

Optional local JSON persistence for EventSourcing event streams, atomic multi-stream batches and operation journal receipts. Core remains filesystem-independent.

## Package and installation

Version **0.3.0** targets **.NET 10** and is licensed under MIT. Configure the [GitHub Packages NuGet feed](https://nuget.pkg.github.com/GaelOn/index.json) and an authorized package-reading credential before installing:

```bash
dotnet add package EventSourcing.Storage.Json --version 0.3.0
```

Keep credentials out of source control. Public repository visibility does not imply anonymous package restore. Alternatively, build the repository in Release and use its generated packages as a local feed; see the [root guide](https://github.com/GaelOn/EventSourcing/blob/main/README.md).

## Opening a store

Register every application event type with a stable discriminator before constructing the store:

```csharp
using EventSourcing.Storage.Json;

// Posting is an application-defined immutable event with JSON-compatible
// construction and cloning. See the complete transaction consumer below.
var codec = new JsonEventCodec().Register<Posting>("posting/v1");
using var store = new JsonEventStreamStore("data/events.json", codec);
```

This is an integration fragment, not a standalone program. The [complete transaction consumer](https://github.com/GaelOn/EventSourcing/blob/main/validation/TransactionConsumer/Program.cs) defines the event, performs an atomic operation, reopens the store and verifies consumed-operation replay.

The codec includes the reserved operation-journal mapping. The store takes a frozen copy of the registry. Unknown discriminators, unsupported document schemas and invalid retained histories fail explicitly. Payload deserialization rejects unmapped members; the registry does not infer semantic identity or perform automatic schema upgrades.

## Ownership and durability

One store instance holds an exclusive OS-level lease for the configured local path throughout its lifetime. Share that instance for in-process callers and dispose it after active work completes. A competing instance/process cannot own the same configured path concurrently.

Use a local filesystem supporting atomic same-directory replacement. Do not bypass the adapter, edit the live document or access it through unsupported aliases. The OS releases ownership when the process dies; that does not automatically recover abandoned operation claims.

Each append validates the candidate histories and operation evidence, serializes the complete document, flushes a staging file, then replaces the committed document. This provides the documented process-crash publication boundary. Power-loss and network-filesystem durability are not promised. The store does not fsync the parent directory after replacement.

## Atomicity and retries

All append participants are checked under the store gate before publication. Unchanged participants still assert their expected versions. A rejected batch does not publish a partial update.

Raw writes retain strict expected-version semantics. Do not blindly retry a raw save after an uncertain acknowledgement. Use the operation coordinator and stable journal identity when semantic retry recognition is required.

After reopening, explicitly run coordinator recovery according to the [transactional contract](https://github.com/GaelOn/EventSourcing/blob/main/docs/transactional-idempotency.md) before work admission. Keep the journal ID stable. Do not reclaim live work merely because another coordinator object exists.

Cancellation before publication can prevent the commit. Once publication completes, cancellation does not retract it. Diagnostic observer exceptions after replacement can model an acknowledgement lost after a successful durable write.

## Operating limits

- The entire document is held in memory and rewritten on each write, including journal transitions; cost grows with retained store size.
- This is not an append-only log, distributed database or multi-process writer service.
- Event and journal data are not encrypted by this adapter. Directory access, backups, confidentiality and secret management are operator responsibilities.
- There is no automatic event/schema migration, retention policy or arbitrary content fingerprint.
- Corrupt storage must be diagnosed; deleting data or resetting receipts is not a safe retry procedure.

Measure expected history size and workload before choosing the adapter for an application.

## Further reading

- [Transactional operations and recovery](https://github.com/GaelOn/EventSourcing/blob/main/docs/transactional-idempotency.md)
- [Raw append contract](https://github.com/GaelOn/EventSourcing/blob/main/docs/raw-append-contract.md)
- [Root architecture guide](https://github.com/GaelOn/EventSourcing/blob/main/README.md)

Licensed under MIT, with repository license included in the package.

