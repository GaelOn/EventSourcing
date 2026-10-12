# Transaction package consumer

This synthetic ledger executable references generated 0.3.0 NuGet packages only.
It validates atomic multi-stream publication, semantic duplicate discard, persisted
reload and replay suppression without source project references or a DI dependency.

```sh
dotnet restore validation/TransactionConsumer --configfile validation/TransactionConsumer/NuGet.Config --packages "$PWD/artifacts/consumer-packages"
dotnet run --project validation/TransactionConsumer -c Release --no-restore -p:RestorePackagesPath="$PWD/artifacts/consumer-packages"
```

Build the solution in Release first. No registry publication is needed.
