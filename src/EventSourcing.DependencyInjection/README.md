# EventSourcing.DependencyInjection

Optional startup composition for core's pure, versioned event handlers. The adapter references
`EventSourcing` and `Microsoft.Extensions.DependencyInjection.Abstractions`; it does not reference
the container implementation. Applications select a compatible implementation. Core remains
usable without this package, DI abstractions, or a container.

## Manual composition remains supported

```csharp
var rules = EventHandlerDispatcher<Inventory, InventoryEvent>.CreateBuilder("inventory/v1")
    .Register<Received>(new ReceiptHandler(new ReceiptConfiguration(3)))
    .Register<Renamed>(new RenameHandler())
    .Build();
var stream = new EventStream<Inventory, InventoryEvent>(id, opening, rules);
```

The integration fixtures provide these complete example types. `Inventory` owns an immutable
`Item`; events carry receipt quantities or both previous and new names. The handler assembly
references core only.

## Constructor injection and explicit version selection

```csharp
using EventSourcing.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

services.AddSingleton(new ReceiptConfiguration(3));
services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1", handlers => handlers
    .AddHandler<Received, ReceiptHandler>(ServiceLifetime.Scoped)
    .AddHandler<Renamed, RenameHandler>(ServiceLifetime.Scoped),
    dispatcherLifetime: ServiceLifetime.Scoped);

services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v2", handlers => handlers
    .AddHandler<Received, DoubleReceiptHandler>()
    .AddHandler<Renamed, RenameHandler>());

using var provider = services.BuildServiceProvider(new ServiceProviderOptions
{ ValidateScopes = true, ValidateOnBuild = true });
using var scope = provider.CreateScope();
var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
var stream = new EventStream<Inventory, InventoryEvent>(id, opening, rules);
var saved = await stream.HandleAsync(new Received(eventId, 2), store);
var snapshot = saved.CaptureSnapshot(schemaVersion: 1);
```

The dispatcher is a keyed service with the ordinal rule-version string as its key. The state and
stream-event types also distinguish registrations. `[FromKeyedServices("inventory/v1")]` can
inject `EventHandlerDispatcher<Inventory, InventoryEvent>` directly into an application service.
No unkeyed default dispatcher is registered. An unknown version fails explicitly.

Replay selects rules explicitly:

```csharp
var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>(definition.RuleVersion);
var replayed = EventStream<Inventory, InventoryEvent>.Replay(id, definition, rules, history);
var restored = EventStream<Inventory, InventoryEvent>.Replay(snapshot, expectedSchemaVersion, rules, suffix);
```

Registering the same state/stream-event/rule-version dispatcher twice is rejected. A second
mapping for the same concrete event within one registration is rejected, even if it names the
same implementation. Failed mapping configuration publishes no adapter service descriptors.
The startup builder freezes when registration completes; keep one entry point per rule set.
Different versions coexist rather than silently replacing one another. Version identities must
include handler implementation and immutable configuration; retain old implementations for replay.

## Client-library registration entry point

A business library can contain core-only handlers and expose an optional integration module:

```csharp
public static IServiceCollection AddFamilyHorizonAccounts(this IServiceCollection services)
{
    services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1", handlers => handlers
        .AddHandler<Received, ReceiptHandler>()
        .AddHandler<Renamed, RenameHandler>());
    services.AddSingleton(new ReceiptConfiguration(3));
    return services;
}
```

The consuming application calls `services.AddFamilyHorizonAccounts()` once. A core-only
application can use the same business handlers through manual composition. The test fixtures
separate the business handler assembly from the client registration assembly to exercise this boundary.
Registration is explicit and typed; this release does not scan assemblies or infer ambiguous mappings.

## Lifetimes and retained instances

Both dispatcher and handler lifetimes are explicit options and default to scoped. Handlers are
registered as keyed concrete implementations isolated by state, stream-event and rule-version.
The adapter resolves constructor dependencies when constructing the dispatcher. One implementation
registered for several event interfaces is resolved once per dispatcher construction. Every mapping
for that implementation must declare the same lifetime.

| Lifetime | Dispatcher reuse | Handler reuse |
| --- | --- | --- |
| Singleton | One per rule registration/provider | One per implementation/rule registration/provider |
| Scoped | One per rule registration/scope | One per implementation/rule registration/scope |
| Transient | New for each resolution | New for each dispatcher construction, retained for its lifetime |

A transient handler retained by a singleton dispatcher effectively lives for that dispatcher's
lifetime. Its dependencies must support that use. Never let singleton dispatchers or singleton
handlers capture scoped dependencies. Enable **both `ValidateScopes` and `ValidateOnBuild`**.
The container can validate constructor dependencies during build; dispatcher factory dependencies
are checked when the dispatcher is resolved. Resolve scoped dispatchers inside their required scope.
Container implementations must honor keyed services, scope validation and disposal semantics.

A lifecycle lease guards all registered transitions. Disposing the creating scope invalidates scoped
and transient dispatchers, including streams retaining their reducer. A singleton dispatcher belongs
to the root provider and survives child-scope disposal; disposing the provider invalidates it.
Transitions then throw `ObjectDisposedException` before invoking handlers. Do not dispose a scope
concurrently with a transition or export a scoped stream into a longer-lived application service.
The container owns and disposes retained disposable handlers; the adapter does not double-dispose them.
Already computed immutable state remains readable; the guard protects further business transitions.

`Do`, `Undo` and replay use retained instances without container lookups or registration changes.
Handlers must remain pure, deterministic, reentrant, and immutable in configuration. Dependency
injection does not make clocks, filesystem access, mutable options or external effects suitable
for calculation handlers. A plain immutable configuration object is preferable to reading live options.

Immediate reversal must satisfy `Undo(Do(state, event), event) == state` under the handler's
preconditions. Events must include previous values when necessary. Pure `Undo` does not change a
stream's history or pending events. Persisted business cancellation appends a new compensating event.

## Validation and packages

Real-container integration tests cover external implementations, injected configuration, client
registration, duplicates/missing handlers, lifetimes/disposal, retained handlers, rule selection,
owned entity state through persisted replay, and container-query-free forward/reverse transitions.
The implementation container package is referenced only by tests and consumer applications.

See `validation/DependencyInjectionConsumer` for a package-only DI consumer and
`validation/PackageConsumer` for a core-only manual-composition consumer. Generated packages
include XML IntelliSense documentation and are licensed under MIT.

Microsoft documents [keyed services and scope validation](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection).
