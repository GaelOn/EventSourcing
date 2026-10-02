# Aggregate and stream lifecycle

`EventSourcedAggregate<TState>` owns one authoritative immutable state and bound rules, paired with one
state-free heterogeneous `EventStream`. `Version` includes pending events, `PersistedVersion` is the last
commit, and `HistoryStartVersion` precedes retained `Events`. Pending collections are immutable views.

Opening construction starts at zero. Replay and snapshot restoration produce validated historical pairs.
The protected root-copy constructor preserves an existing framework frame; it cannot accept independent
state/history values. Subclass fields must be immutable and entity access must project `State`.

`Apply` accepts/tags the clone before reducing and publishes only a complete root. `ApplyBatch` uses local
immutable branches and publishes no partial result on failure. `SaveAsync` and aggregate batch saving
prepare saved views before I/O using framework-controlled runtime-root copies, so no reducer, subclass
constructor or wrapper callback runs after durable commit. Continue using the returned saved root.

Public `Stream.Append` is a low-level infrastructure capability. It changes only history and applies no
business rules; it cannot replace the containing root's frame. Normal consumers use business-root operations.
This API is not an authorization boundary. A façade can restrict exposed business methods where required.

Rules identify implementation and immutable configuration. Capture derives rule identity from the root;
explicit labels must match. Retain historical rules and original opening definitions. No generic mechanism
can prove that a reducer implements its identity or arbitrary manually constructed snapshot data matches
history. These are documented application/adapter trust boundaries.

Capture only committed snapshots and save them separately. Checkpoint failure never undoes event commits.
Restore a compatible state schema/rule identity using a complete suffix and every earlier event ID; reject
invalid cursors, identities, duplicates and corruption. No compatible checkpoint means full replay. For
several roots, select checkpoints then obtain all suffixes in one consistent batch read.

Typed `Do` and `Undo` are pure, reentrant transitions. Exact-type registry mappings freeze on build and
retained handlers make no service lookups during transitions. Events include reversal information such as
previous values. Immediate reversal restores the entire input state under explicit handler preconditions.
Pure `Undo` returns state only and never edits history or creates a mismatched root. Persist cancellation
with a new compensating event/new ID, preserving the original and its causal reference.

Durable adapters own stable event discriminators, payload schemas and explicit migration/rejection paths.
The framework validates clone identity/type/metadata, but cannot generically prove deeply immutable or
equivalent payloads. All clients must honor that contract. Structured exception properties avoid message parsing.
