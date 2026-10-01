# Stream and aggregate lifecycle

`EventSourcedAggregate` projects the complete immutable stream state. `Version` includes accepted
pending events; `PersistedVersion` is the last successful event commit; `HistoryStartVersion`
is the checkpoint position preceding retained `Events`. `PendingEvents` and `HasPendingChanges`
show unsaved work. Saving returns a new saved stream/root; always retain the returned value.
Failures preserve the original branch and its pending events.

Public `Stream.Apply()` is intentional for generic scenario calculation, diagnostics and composition.
It returns an independent immutable branch and cannot modify the containing root. Applications that
require a restricted business API should expose their own façade rather than distribute the generic
root/store. The library does not provide an authorization boundary or enforce application business
operations outside the supplied reducer.

Bind a pure reducer with `EventReducer(stateRulesVersion, reduce)` or use an identified
`EventHandlerDispatcher`. Construct streams with that mechanism and call `CaptureSnapshot(schemaVersion)`;
the rule identity follows the mechanism through application, saving and replay. The version identifies
implementation **and immutable configuration**; applications must assign it honestly and retain old rules.
Legacy delegate/string overloads remain for compatibility and cannot infer rule identity from code.
An already bound stream rejects conflicting snapshot labels.

Capture a snapshot only after saving events. Save it separately; failed snapshot writes never undo
committed events. Keep opening inputs, rule identities and full event history. Restore using a compatible
state schema and rule version, a suffix beginning at the snapshot's stream position, and every earlier
ID for duplicate rejection. Multi-root restoration must obtain suffixes from one consistent batch read.

Typed handlers implement `Do` and `Undo` as pure, reentrant transitions. Compose one exact runtime
handler per event type with a startup builder and freeze it with `Build`. Missing and duplicate
registrations fail explicitly. Do not mutate input state/events, change handler configuration after
registration, read the clock or perform external effects.

Events must include all reversal information, including previous values for replacements. A handler
must state and enforce reversal preconditions. Immediate reversal must satisfy
`Undo(Do(state, event), event) == state` for valid inputs. Reversing an older event after intervening
changes is not generally valid: a replacement handler checks that the current value equals its recorded
new value before restoring the previous value. Pure `Undo` does not edit stream state/history or persist
anything. Persisted business cancellation must append a **new compensating event with a new ID** and
retain the original event. Compensation can have its own business preconditions and causal reference.

Persistence adapters own stable event discriminators and payload schema versions. Do not serialize CLR
assembly-qualified type names as a durable contract. Fail explicitly on unsupported event kinds or
schemas; migrations and upcasters belong to the adapter/application, not the generic stream engine.

Structured exception properties expose duplicate IDs, expected/actual positions, stream identity,
clone types, recorded/reducer rules and snapshot schemas without parsing localized messages.
