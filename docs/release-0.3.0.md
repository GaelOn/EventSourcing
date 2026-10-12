# EventSourcing 0.3.0 package handoff

Additive keyed processing contracts and optional exclusive-process JSON storage.
See [transactional-idempotency.md](transactional-idempotency.md) for identity, atomicity,
replay, recovery and compatibility boundaries. Business event metadata remains unchanged.
No Family Horizon code, registry publication, tag or PR creation is performed here.

Build and validate the exact source before generating packages:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx -c Release --no-restore -warnaserror
dotnet test EventSourcing.slnx -c Release --no-build --no-restore
```

Release produces `EventSourcing`, `EventSourcing.DependencyInjection`,
`EventSourcing.Compatibility` and `EventSourcing.Storage.Json` version 0.3.0,
including symbols, XML docs, README and MIT license, under `artifacts/packages/`.
The four package-only consumers (manual/core, DI, compatibility and transactions)
verify the generated packages with an isolated cache. CI runs them after Release tests.
The transaction consumer restores from local generated packages only.

Family Horizon can validate a locally built 0.3.0 package through a temporary local
NuGet feed/isolated restore without changing any published version. Its checked-in
private feed/clean CI needs actual publication before consuming that version normally.
Its follow-up must pin the final tested commit, not an assumed remote package.
The final implementation report records that commit and all milestone SHAs.

After green CI, review and merge, an explicitly authorized operator may publish
using the existing GitHub release workflow (run from the EventSourcing checkout):

```sh
git switch main
git pull --ff-only --prune origin main
# Confirm main contains the reviewed milestone before tagging.
git tag -a v0.3.0 -m 'EventSourcing 0.3.0 — transactional idempotency'
git push origin v0.3.0
gh release create v0.3.0 --repo GaelOn/EventSourcing --verify-tag \
  --title 'EventSourcing 0.3.0' --notes-file docs/release-0.3.0.md
```

The release-published workflow builds/tests the tag, publishes matching `.nupkg`
files to `https://nuget.pkg.github.com/GaelOn/index.json`, and attaches packages/symbols.
These are handoff commands, not actions already executed. Never overwrite an existing tag/package.

## Executed validation

On 12 October 2026, solution restore, Debug and Release builds with warnings as
errors succeeded (zero warnings/errors). Full correctness suites passed in both
configurations: **200 core + 21 DI tests**. Release TRX evidence is retained under
ignored `artifacts/test-results/`. This includes three actual child-process kills
(staged, before replacement, after replacement), restart/recovery, exact operation
receipt/event identity checks and cross-process ownership fencing on macOS.
Linux CI runs the same suite; Linux execution has not been claimed locally.

All four isolated-cache generated-package consumers passed: manual/core, DI,
compatibility and durable transaction/replay. All four 0.3.0 packages were inspected
for matching versions/dependencies, README, MIT license, XML docs and symbol archives.
Core has no package dependency; JSON depends only on matching core; DI's existing
abstractions dependency remains unchanged. Package publication is still pending.
The original M0 run had two executed assertion failures and two passes, recorded
in [the audit](transactional-idempotency-audit.md); all retained regressions are green.
