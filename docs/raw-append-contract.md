# Raw append contract and operation protocol verification

## Decision: B0, strict raw appends

Inspected EventSourcing `32586f5256d427419025ac4b6134ae16f8fc9a8f`
(current origin/main before this test change) and Family Horizon
`3091968a311351810c2fb957e8adccce36d34f02`. Family Horizon's
`src/FamilyHorizon.Domain/FamilyHorizon.Domain.csproj` consumes published
**EventSourcing 0.3.0**, not this branch's source. This branch retains package
version 0.3.0; no publication, API, production behavior or format change is made.
The inspected source is the merged implementation of that release; this review
is source inspection, not a byte comparison of downloaded NuGet assemblies.

A successful raw append means the request passed expected-version checks and
committed. Repeating it with its old expected version throws
`StreamConcurrencyException`, even with matching event IDs. Rejected stale
retries leave history unchanged. An exception after publication can leave an
**unknown outcome**; it does not prove that nothing was written. In particular,
the JSON adapter's documented AfterReplace observer can throw after publication.
The interface's general failure-preservation wording must be read alongside
that explicit publication boundary, not as proof of an absent commit after a
lost acknowledgement. Reload and inspect evidence; do not blindly replay a raw
save and treat a conflict as either successful retry recognition or a new write.

`EventStream.SaveAsync` and aggregate saves retain their immutable pending
branch on failure; they do not resolve uncertain commits. Both shipped stores
check expected versions before validating new IDs and metadata, under their
write gate. Atomic appends validate every participant, including an unchanged
read dependency. Independently matching per-stream histories cannot establish
that a particular atomic batch was previously committed.

The operation protocol is a different, explicit contract. `OperationProcessor`
admits a stable client key, stages isolated consequences, and commits business
appends with an owned journal receipt. `LocalEventTransactionCoordinator`
resolves acknowledgement loss on claim, commit and rollback by rereading journal
state and checking the attempt (and owner session for a claim). Recovery is an
explicit call that rolls back abandoned older-session claims; timeout does not
steal an active claim. Receipts retain participant versions and ordered event
IDs. A no-op can be consumed without business events.

| Option | Meaning and cost | Decision |
| --- | --- | --- |
| B0 | Strict concurrency-checked raw commits; stale repeats conflict; uncertain post-publication outcomes require inspection. | Retain. |
| O1 | Single-stream matching IDs, concrete types and metadata at expected positions, without payload equality; atomic batches stay strict. | Separate feature only. |
| O2 | Single-stream matching identity and content, with equality defined across both stores; does not prove earlier atomic-batch identity. | Separate feature only. |
| O3 | Durable batch identity bound to all participants, preconditions and content; requires API, persisted-format and compatibility design. | Separate feature only. |

The supplied `eventsourcing-idempotent-append.patch`, `IdempotentAppendTests.cs`
and `OperationProtocolModelTests.cs` were reviewed as unexecuted proposals.
The patch's IsRecorded/IsRecordedBatch shortcuts are not adopted: payloads are
not compared, separately committed streams can appear to match a batch, and
ignoring event-less participants bypasses preconditions. Its success-oriented
raw tests are replaced with B0 assertions. The random walk's action distribution
and fixed seeds are retained, but its count/membership oracle is strengthened
and clean reopening is called **session replacement**, not a process crash.

## Verified consumers

Paths below are relative to their respective repositories at the commits above.
This inventories reachable source paths inspected here, not unknown downstream
package users. If-Match/version checks protect concurrency; they do not by
themselves recognize an acknowledged-lost operation.

| Caller / source | Persistence path and existing protection | Raw retry need / unresolved usage |
| --- | --- | --- |
| ES `src/EventSourcing/Transactions/LocalEventTransactionCoordinator.cs` | Begin/Rollback use raw AppendAsync; Commit uses atomic business+journal append. Catch paths reread the journal and match the owned attempt. Recover appends rollback markers explicitly. | No raw success recognition needed for the three acknowledged transitions. Recovery itself may surface an uncertain write; reopen/inspect/recover is explicit. |
| ES `src/EventSourcing/Transactions/OperationProcessor.cs` | Calls Begin, isolated callback, Stage, Commit; duplicates do not invoke consequences. Failures attempt rollback; unresolved rollback surfaces AggregateException. | Semantic retry uses operation keys and receipts, not raw ID recognition. |
| ES `src/EventSourcing/Streams/EventStream.cs`, `EventStreamBatch.cs`; `ObjectsAggregation/EventSourcedAggregate.cs`, `EventSourcedAggregateBatch.cs` | Raw save with expected versions, immutable pending branches; atomic aggregate batch includes unchanged participants. | General public low-level callers can face uncertain acknowledgement. No demonstrated reachable automatic raw retry requiring O1/O2/O3; consumers must reload/reconcile. Unknown external callers remain unverified. |
| ES `samples/EventSourcing.PackageConsumer/Program.cs`, `validation/PackageConsumer/Program.cs` | Aggregate/raw saves; explicit stale-write rejection examples. | Samples require strict stale rejection, not retry success. |
| ES `validation/DependencyInjectionConsumer/Program.cs` and registration fixtures | Aggregate saves/checkpoints and independently composed handlers. | No retry loop or lost-ack recognition requirement found. Snapshot saves are separate from event appends. |
| ES `validation/TransactionConsumer/Program.cs` | OperationProcessor over local JSON; restart resolves consumed key without callback. | Already uses semantic receipt protocol. |
| FH `Application/ScenarioApplication.cs` ExecuteAsync and `Application/MoneyManagementApplication.cs`; `Adapters/FinancialOperationStore.cs` ExecuteOperationAsync; `Cli/CliRunner.cs` | Initialization and each month use versioned FinancialOperationKeys. Persisted root and receipt commit atomically through OperationProcessor; current host explicitly recovers older-session claims, reloads frozen root, and validates MonthClosed. CLI persistent path uses the same operation path; pure in-memory execution does not save. | No raw retry feature needed for these paths. New delivery event IDs cannot bypass a consumed operation. |
| FH `Application/PlanningRunRecovery.cs`, `Api/PlanningRunWorker.cs` | Durable queued/running jobs resume the same retained run; current completion is reconciled. Financial month execution uses the operation path above. | Job restart and bounded storage-contention retries are not blind raw append retries. |
| FH `Application/PlanningSubmissions.cs` SubmitAsync; `Application/PlanningResults.cs`; `Infrastructure/PlanningFileCoordinator.cs` | Actor+submission key+content are looked up before selection. Accepted result/opening and catalogue share a staged multi-document transaction with durable decision recovery. | Submission recognition belongs to catalogue key/content, not raw event IDs or If-Match. Verify permission on an existing result before return. |
| FH `Adapters/ScenarioRepositories.cs` JsonFinancialRunRepository SaveAsync | Money-management pending MonthClosed routes to ExecuteAsync; other callers fall back to FinancialState.SaveAsync -> raw AppendAsync. CreateAsync delegates to run opening creation. | Fallback remains strict with uncertain-outcome inspection required. Direct domain/test consumers and older/manual paths exist; no blanket semantic guarantee inferred. A future automatic retry requirement needs a scoped caller-level specification. |
| FH `Adapters/JsonFinancialStore.cs` CreateAsync; `Application/ScenarioApplication.cs` SavePlanningOpeningAsync / run creation | Opening definition is a document mutation, requires version zero/no pending events and rejects duplicate run ID. Planning acceptance coordinates it with catalogue; standalone API/CLI creation is distinct. | Not a raw append. Duplicate opening rejection does not establish semantic equality after lost acknowledgement; no feature justified by changing raw stores. |
| FH `Adapters/ScenarioRepositories.cs` JsonScenarioRepository; `Adapters/JsonScenarioStore.cs`; `Application/ScenarioApplication.cs` | Editable scenario save -> aggregate raw save. Store checks version, validates edits, replaces document atomically; application has strong revision checks. | Version checks are not acknowledgement recognition. No automatic identical raw retry found. Need explicit caller workflow before adding an alternate contract. |
| FH `Application/HouseholdApplication.cs`, `PlanningMemberships.cs`, `PlanningCopies.cs`; `Adapters/JsonHouseholdRepository.cs` | Household/profile persistence uses its own audited document update with expected household revision; current-only descriptive profile. | Does not append through EventSourcing stores. Profile/membership concurrency is not semantic idempotency; raw library recognition would not solve it. |
| FH `Domain/Finance/FinancialState.cs` HandleAsync / SaveAsync; adapter tests and CLI direct callers | Immutable aggregate raw save remains available; tests exercise strict conflicts and replay, while normal month runner uses explicit operation keys. | General low-level usage remains supported without promising semantic retries. All external applications beyond these two repositories are outside the inventory. |

Family Horizon source was inspected read-only. Its custom JsonFinancialStore
and JsonScenarioStore implement library ports; they are not either of the two
shipped EventSourcing store implementations. They are not changed or revalidated
by this test-only EventSourcing PR. Existing FH financial idempotency tests are
supporting source evidence, not newly executed validation for this PR.

No verified consumer requires the proposed raw append shortcut. General raw
consumers still have uncertain-outcome handling obligations. If a concrete
consumer requires recognition, specify its semantic key/content and complete
participant preconditions separately before selecting O1, O2 or O3.

## Test scope and reproduction

`StrictRawAppendTests` runs against memory and JSON: single/atomic stale repeats,
changed payloads with reused IDs, independently committed participants, unchanged
history after rejection, event-less preconditions, and raw save acknowledgement
loss. The last case proves publication despite IOException, strict retry conflict,
unchanged document bytes and preservation of the pending immutable branch.

`OperationProtocolModelTests` uses seeds 1–12 and 120 random actions per seed.
Each seed first runs a deterministic sequence guaranteeing abandoned recovery,
stale commit, external append, staged session replacement and lost claim/commit/
rollback acknowledgements. A coverage set asserts those transitions independently
of random-tail frequency. After each step the independent oracle checks complete
ordered business IDs, amounts, reasons and metadata; every journal transition,
key, contract, owner/attempt, metadata, outcome and exact receipt ranges; then
production OperationEvidence.Validate runs as an additional check. Opaque attempt
IDs are learned only from successful admission, not from the production validator.
Failures report seed and ordered action trace. Reproduce a seed, for example:

```sh
dotnet test src/Test/EventSourcing.Tests/EventSourcing.Tests.csproj -c Release --no-build \
  --filter 'FullyQualifiedName~OperationProtocolModelTests&DisplayName~(1)'
```

`OperationProtocolConcurrencyTests` races eight callers against both stores.
Barriers hold the winner's isolated callback until all seven competing calls
return InProgress. The winner commits once; a later delivery is AlreadyConsumed.
No sleeps or callback-per-caller assumption. The test deadline is 20 seconds.

Existing forced-process tests are unchanged. The crash host additionally runs
three generated operations: two fully committed prefix operations, then a third
killed at Staged (seed 17), BeforeReplace (29), or AfterReplace (43). Parent/child
use the existing runtime/deps invocation and READY barrier. The parent inspects
durable evidence before explicit recovery, verifies the prefix, retries the
unfinished key, and compares complete payload/identity histories and receipts.
Staged/BeforeReplace are known unpublished; AfterReplace is published even though
the caller has no acknowledgement. These explicit barriers have known outcomes;
no arbitrary OS scheduling boundary or power-loss guarantee is inferred. Clean
session replacement tests are separate. Each child has bounded readiness/exit
waits and finally cleanup without masking the original failure.

These finite seeds, schedules and publication boundaries do not exhaust every
interleaving or filesystem failure. No production defect has been reproduced by
the tests at implementation time; actual executed results are recorded in the PR.

Repository validation follows `.github/workflows/dotnet.yml`: pinned SDK,
Release build with warnings as errors, full solution tests including real crash
children, and all four isolated-cache package consumers. There is no additional
EventSourcing quality script or configured pre-push hook in the inspected repo;
no hook is disabled. Family Horizon's application-specific quality script does
not validate this independent repository and is not substituted for its CI checks.
