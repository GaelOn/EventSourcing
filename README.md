# EventSourcing

Standalone .NET 10 library for immutable event streams, deterministic replay, and atomic multi-stream persistence contracts.

From this repository root:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx --no-restore
dotnet test EventSourcing.slnx --no-build
dotnet build EventSourcing.slnx -c Release
```

Release packages are generated in `artifacts/packages/`. The solution and tests have no dependency on the Family Horizon repository.

See [library documentation](src/EventSourcing/README.md) for usage, concurrency contracts, and limitations. The file-backed adapter and file store under `src/Test` are test fixtures, not production persistence implementations.
