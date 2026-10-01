# NuGet consumer verification

This executable references the compatibility NuGet package and consumes core transitively. It has no project references. Run after building Release packages, using an isolated package cache so a previously cached alpha cannot hide a packaging defect:

```sh
dotnet restore samples/EventSourcing.PackageConsumer --source ./artifacts/packages --packages ./artifacts/consumer-packages
dotnet run --project samples/EventSourcing.PackageConsumer -c Release --no-restore -p:RestorePackagesPath="$PWD/artifacts/consumer-packages"
```

The program checks handlers and reversal, root/entity state, automatic rule identity, snapshot/full replay equivalence, duplicate protection across a checkpoint, atomic batching and consistent reads, structured diagnostics, and compatibility interoperability. Its version must track the generated packages. It is deliberately outside the solution: correctness CI builds packages first and package consumption can then be verified independently.
