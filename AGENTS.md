# EventSourcing contributor instructions

- Use .NET 10, C#, nullable reference types, `.slnx`, and xUnit.
- Keep this repository independently buildable; do not reference files or projects outside it.
- Prefer immutable state and events, explicit caller-supplied IDs, deterministic reducers, and atomic expected-version checks.
- Add behavior-focused failing tests before changing behavior. Never weaken a valid test to obtain a green build.
- Preserve the all-or-nothing batch contract and consistent multi-stream reads. Add reconciliation and concurrency tests when changing those guarantees.
- Run `dotnet restore EventSourcing.slnx`, `dotnet build EventSourcing.slnx --no-restore`, and `dotnet test EventSourcing.slnx --no-build` before completion.
- Keep generated `bin`, `obj`, test output, packages, and secrets out of Git. Review the staged diff before committing.
- Document public API changes in the README. Keep the core library independent of filesystem and application-specific dependencies.
