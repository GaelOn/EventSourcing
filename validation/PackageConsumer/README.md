# Generated package consumer

This project deliberately has only a `PackageReference`, no project references. Its local-only
NuGet configuration prevents accidentally substituting a published package. From the repository root:

```sh
dotnet build EventSourcing.slnx -c Release -warnaserror
dotnet restore validation/PackageConsumer --packages artifacts/consumer-packages --force
dotnet run -c Release --no-restore --project validation/PackageConsumer
```

Use an empty `artifacts/consumer-packages` directory when validating a rebuilt package with the same
version. The consumer exercises public handlers and reversal, aggregate lifecycle, automatic rule
identity, snapshots, duplicate/stale-write diagnostics, and atomic operations. It stays outside the
solution so ordinary builds do not require a package that has yet to be generated.
