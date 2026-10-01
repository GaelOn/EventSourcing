# Stream performance diagnostic

Run from the repository root after a Release solution build:

```sh
dotnet run --project benchmarks/EventSourcing.Benchmarks -c Release --no-build
```

This console program has no additional dependencies. It warms both operations with 100 events, prepares caller IDs outside measurement, collects garbage before each operation, and measures elapsed time and process-wide allocations. It checks the resulting balance/version and, for apply-and-save, the final stored history count. CSV goes to standard output.

The two workloads are pure `Apply` and `Apply` followed by `SaveAsync` for every event. The latter intentionally exposes accumulated history costs in the in-memory store. The measured sizes cover small streams through a larger scenario history; they are diagnostic sizes, not validated household workload estimates.

## Local comparison

Measured on the same macOS machine with .NET SDK 10.0.400 in Release mode on 2026-10-01, before and after structural sharing and incremental store validation:

| Events | Operation | Before ms | After ms | Before allocated bytes | After allocated bytes |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | Apply | 34.15 | 3.72 | 8,575,976 | 1,953,288 |
| 5,000 | Apply | 259.09 | 23.74 | 202,698,704 | 11,679,960 |
| 10,000 | Apply | 471.83 | 37.56 | 805,200,312 | 24,996,888 |
| 1,000 | Apply and save | 119.76 | 8.64 | 79,950,816 | 3,557,080 |
| 5,000 | Apply and save | 1,059.09 | 67.56 | 2,027,053,176 | 20,465,880 |
| 10,000 | Apply and save | 4,033.45 | 65.37 | 8,067,348,632 | 42,808,640 |

These are single diagnostic runs. JIT tiering, garbage collection, machine load, and GUID hash distributions affect timings; the smaller row taking longer than a larger one illustrates that noise. Allocation growth is more informative here than precise speed ratios. Use repeated measurements for performance decisions. CI does not assert timings.

The optimization replaces whole-history arrays/scans with shared immutable lists and event-ID sets; memory-store writes stage a shared immutable ledger and validate only new events. State/events still require deep immutability. Full history remains in memory, and replay remains proportional to history size. The JSON integration fixture still rewrites its complete document and is deliberately excluded from this benchmark; its performance is not improved by the memory-store optimization.
