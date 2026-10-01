# Package-only DI consumer

No project references. NuGet source mapping requires generated EventSourcing packages from the local
package output and permits Microsoft/System dependencies from NuGet. The core-only consumer remains
in `../PackageConsumer`, whose local-only configuration cannot restore a transitive DI dependency.

From the repository root:

```sh
dotnet build EventSourcing.slnx -c Release -warnaserror
dotnet restore validation/DependencyInjectionConsumer --packages artifacts/di-consumer-packages --force
dotnet run -c Release --no-restore --project validation/DependencyInjectionConsumer
dotnet restore validation/PackageConsumer --packages artifacts/core-only-consumer-packages --force
dotnet run -c Release --no-restore --project validation/PackageConsumer
```

Use fresh caches when rebuilding a package without changing its version. These consumers stay
outside the solution because their packages must be generated before they can be restored.
