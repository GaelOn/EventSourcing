# Refactor diagnostic baseline — 2 October 2026

The [main report](2026-10-02.md) and [CSV](2026-10-02.csv) contain 34 measurements. The [batch report](2026-10-02-batch.md) and [CSV](2026-10-02-batch.csv) contain two additional measurements. [Source hashes](2026-10-02-source.sha256) identify the measured core, adapter and benchmark sources; the working tree is based on commit 82317e10f30e5b26744e0aacc2a87a0b3556323b.

## Reproduction

Build Release first, then run from the repository root:

```sh
dotnet build EventSourcing.slnx -c Release -warnaserror
dotnet run --project benchmarks/EventSourcing.PerformanceTests -c Release --no-build -- --verify
EVENTSOURCING_BENCHMARK_PROFILE=baseline dotnet run --project benchmarks/EventSourcing.PerformanceTests -c Release --no-build -- --filter '*' --job Short --warmupCount 1 --iterationCount 3 --launchCount 1 --artifacts artifacts/benchmarks/reproducible-2026-10-02
EVENTSOURCING_BENCHMARK_PROFILE=batch dotnet run --project benchmarks/EventSourcing.PerformanceTests -c Release --no-build -- --filter '*ApplyAndSave*' '*SaveEachEvent*' --job Short --warmupCount 1 --iterationCount 3 --launchCount 1 --artifacts artifacts/benchmarks/reproducible-batch-2026-10-02
```

BenchmarkDotNet 0.15.8, SDK 10.0.400, runtime 10.0.11, x64 RyuJIT, workstation GC; macOS Tahoe 26.7.1, Intel Core i9-9880H, eight physical and sixteen logical cores. The reports retain the exact environment and job settings.

The main profile uses 10,000 historical events, snapshots at 10% and 90%, one appended event and two atomic participants. The batch profile compares one save of 100 events with 100 individual saves. All 16 combinations of historical count, snapshot position, suffix length and participant count passed workload verification outside timed sections; the entire matrix was not timed.

Fixture IDs are deterministic SHA-256-derived GUIDs generated during setup. Setup also prepares histories, dispatchers and snapshots. Write stores reset outside measured operations. Homogeneous and heterogeneous handler replay both receive arrays, with matching event counts and arithmetic. Managed allocation diagnostics accompany execution time. Snapshot restoration with a preloaded slice is measured separately from restoration including historical identity loading; prefix identity-index construction and historical-ID loading also have dedicated measurements.

## Interpretation and comparison

The [previous baseline](2026-10-01.md) used random GUIDs and the previous API. Its full replay allocated about 2.46 MB; the new report also allocates about 2.46 MB. Older and recent preloaded snapshot restoration retain approximately 288 KB and 32 KB allocations respectively. Including identity loading allocates approximately 1.43 MB and 0.94 MB respectively. Homogeneous and heterogeneous handler replay each allocate approximately 2.94 MB. Exact measurements are retained in the reports.

Execution times vary substantially even for operations whose work is independent of snapshot position: full replay ranges from roughly 4.4 to 23.8 ms. This shared-host run therefore does not establish a speedup or regression against the earlier baseline. Different ID corpora also prevent an isolated before/after attribution. Repeat longer jobs on a quiet controlled host before drawing latency conclusions. The measurements demonstrate that loading historical identities can outweigh suffix reduction, so a preloaded snapshot figure is not an end-to-end restoration figure.

Write benchmarks use one invocation per iteration to prevent accumulated state and duplicate IDs. Their short iterations and three samples produce wide confidence intervals. Allocations represent managed allocations, not retained memory or durable I/O costs. No timing assertions were added to correctness tests or standard CI.
