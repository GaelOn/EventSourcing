# Local diagnostic baseline — 1 October 2026

Results: [Markdown report](2026-10-01.md), [CSV report](2026-10-01.csv).
Source base commit: `fa169c38d028b515cf0a11a5780cb28b179dbbbd` plus the refinement working-tree changes.
[Source hashes](2026-10-01-source.sha256) identify the source recorded with this baseline.

Command from the repository root:

```sh
EVENTSOURCING_BENCHMARK_PROFILE=baseline dotnet run -c Release --no-build --project benchmarks/EventSourcing.PerformanceTests -- --filter '*' --job Short --warmupCount 1 --iterationCount 3 --artifacts artifacts/performance-final
```

BenchmarkDotNet 0.15.8; .NET SDK 10.0.400; runtime .NET 10.0.11, x64 RyuJIT;
Intel Core i9-9880H at 2.30 GHz, 8 physical / 16 logical cores; macOS Tahoe 26.7.1.
Concurrent workstation GC. One launch, one warmup iteration, three measured iterations.
18 measurements completed, covering all nine operations at the older (10%) and recent (90%)
snapshot positions, with 10,000 historical events, suffix length 1 and two atomic participants.
All 16 full-matrix workload configurations were checked separately with `--verify`.
The full matrix is available; it was not timed in this representative baseline.

Full replay averaged 5.0–5.5 ms and allocated about 2.46 MB. Preloaded checkpoint restoration
averaged 0.292 ms with the older checkpoint and 0.037 ms with the recent checkpoint, allocating
288 KB and 32 KB respectively. Including historical ID loading and validation averaged roughly
6 ms and allocated 0.94–1.43 MB. The prefix-only identity-index benchmark makes its O(prefix)
cost visible separately. These results support examining identity-index loading before treating
snapshot replay savings as end-to-end savings; they do not establish a production latency target.

This is a shared-host diagnostic, not a controlled laboratory comparison. The runner could not
raise process priority. Three measurements give wide confidence intervals. BenchmarkDotNet
reported short-iteration warnings for write workloads, whose one-invocation setup prevents
state accumulation and duplicate IDs. Treat those timings as noisy, and use a longer job with
batched independent write workloads for precise comparisons. Allocation counts are managed
allocations, not retained memory, disk I/O or network costs. No timing thresholds run in CI.

See the [methodology](../README.md) and BenchmarkDotNet's
[memory diagnostics documentation](https://benchmarkdotnet.org/articles/configs/diagnosers.html).
