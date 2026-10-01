# EventSourcing performance workloads

This development-only BenchmarkDotNet project measures execution time and managed allocations
with `MemoryDiagnoser`. It has no reference from the core package or ordinary correctness tests.
No timing assertion belongs in the correctness suite.

Run from the repository root in Release:

```sh
dotnet run -c Release --project benchmarks/EventSourcing.PerformanceTests -- --filter '*' --job Short
```

The full matrix varies historical event count (100, 10,000), snapshot position (10%, 90%),
new suffix length (1, 100), and atomic participant count (2, 8). Snapshot replay suffix length
is `HistoricalEventCount - snapshot cursor`; `SuffixLength` controls new live events. Both
older and recent checkpoints are measured at each history size.

`GlobalSetup` generates IDs, prepares committed histories/checkpoints and checks each workload.
Write workloads reset their stores in `IterationSetup`, outside measured operations; cleanup
checks persisted count, state and version. BenchmarkDotNet limits iteration-setup workloads
to one invocation per iteration so writes cannot accumulate or reuse IDs. Applying events,
creating pending branches and atomic storage append are deliberately measured together.

`SnapshotRestore` uses an already loaded, validated suffix and ID index.
`SnapshotLoadIncludingHistoricalIds` includes the in-memory store range read, historical ID
loading/validation and restoration. `LoadHistoricalIds` isolates construction/validation of
that ID index and the suffix. These expose costs that a reducer-only comparison would omit.
They do not model disk/network latency; durable adapter measurements are a separate concern.

Retain the Markdown and CSV reports with runtime, OS, CPU, parameters, command and commit in
`baselines/`. Use the same machine/runtime/configuration for comparisons. Short runs establish
a local diagnostic baseline; use the default job on an idle machine for performance decisions.
