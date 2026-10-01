# EventSourcing

Standalone .NET 10 library for immutable event streams, deterministic replay, and atomic multi-stream persistence contracts.

From this repository root:

```sh
dotnet restore EventSourcing.slnx
dotnet build EventSourcing.slnx --no-restore
dotnet test EventSourcing.slnx --no-build
dotnet build EventSourcing.slnx -c Release
```

Core and optional compatibility Release packages (`.nupkg` and `.snupkg`) are generated in `artifacts/packages/`. The current prerelease is `0.1.0-alpha.2`; see the library documentation for breaking API migration instructions. The solution and tests have no dependency on the Family Horizon repository.

See [library documentation](src/EventSourcing/README.md) for usage, concurrency contracts, and limitations. The file-backed adapter and file store under `src/Test` are test fixtures, not production persistence implementations.

## CI artifacts

The GitHub Actions workflow runs on pushes and pull requests to `main`, and supports manual runs. It selects the SDK from `global.json`, builds Release with warnings as errors, and tests the Release binaries. Test results are uploaded even after failures when a TRX file exists. Successful builds and tests upload both NuGet packages and portable symbols for 30 days; test results are retained for 14 days. Artifacts are downloadable from the workflow run. No package-feed publication is configured.

## Performance

Run `dotnet run --project benchmarks/EventSourcing.Benchmarks -c Release --no-build` after the Release solution build. See [benchmark notes](benchmarks/EventSourcing.Benchmarks/README.md) for methodology and measurements.
