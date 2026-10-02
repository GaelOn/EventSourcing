using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.ExternalHandlers.RegistrationFixture;
using EventSourcing.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection.Tests;

public sealed class RetentionTests
{
    [Fact]
    public void Forward_and_reverse_use_retained_instances_without_any_further_container_queries()
    {
        var services = new ServiceCollection().AddFamilyHorizonAccounts();
        var descriptor = services.Single(d => d.ServiceType == typeof(EventHandlerDispatcher<Inventory>));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        using var scope = provider.CreateScope();
        var observed = new ObservedProvider(scope.ServiceProvider);
        // Invoke the registered dispatcher factory with an observed real-container scope.
        var dispatcher = (EventHandlerDispatcher<Inventory>)descriptor.KeyedImplementationFactory!(observed, "inventory/v1");
        var queriesAtConstruction = observed.Queries;
        Assert.True(queriesAtConstruction > 0);
        var registrationsAtConstruction = services.Count;
        var opening = new Inventory(new("Original", 10));
        IEvent[] events = [new Received(Guid.NewGuid(), 2), new Renamed(Guid.NewGuid(), "Original", "Next")];
        foreach (var e in events) Assert.Equal(opening, dispatcher.Undo(dispatcher.Do(opening, e), e));
        Assert.Equal(queriesAtConstruction, observed.Queries);
        Assert.Equal(registrationsAtConstruction, services.Count);
        Assert.Equal(new Inventory(new Item("Original", 10)), opening);
    }

    [Fact]
    public void Container_disposes_a_retained_multi_interface_transient_handler_once_at_scope_end()
    {
        var probe = new DisposalProbe();
        var services = new ServiceCollection(); services.AddSingleton(probe);
        services.AddEventHandlerDispatcher<Inventory>("disposal/v1", h => h
            .AddHandler<Received, DisposableHandler>(ServiceLifetime.Transient)
            .AddHandler<Renamed, DisposableHandler>(ServiceLifetime.Transient));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory>("disposal/v1");
        Assert.Equal(1, probe.Constructed); Assert.Equal(0, probe.Disposed);
        scope.Dispose();
        Assert.Equal(1, probe.Disposed);
        Assert.Throws<ObjectDisposedException>(() => dispatcher.Do(new(new("Original", 10)), new Received(Guid.NewGuid(), 1)));
    }

    private sealed class ObservedProvider(IServiceProvider inner) : IKeyedServiceProvider
    {
        public int Queries { get; private set; }
        public object? GetService(Type serviceType) { Queries++; return inner.GetService(serviceType); }
        public object? GetKeyedService(Type serviceType, object? serviceKey)
        { Queries++; return ((IKeyedServiceProvider)inner).GetKeyedService(serviceType, serviceKey); }
        public object GetRequiredKeyedService(Type serviceType, object? serviceKey)
        { Queries++; return ((IKeyedServiceProvider)inner).GetRequiredKeyedService(serviceType, serviceKey); }
    }
    public sealed class DisposalProbe { public int Constructed { get; set; } public int Disposed { get; set; } }
    public sealed class DisposableHandler : IEventHandler<Inventory, Received>, IEventHandler<Inventory, Renamed>, IDisposable
    {
        private readonly DisposalProbe _probe;
        public DisposableHandler(DisposalProbe probe) { _probe = probe; probe.Constructed++; }
        public Inventory Do(Inventory s, Received e) => s;
        public Inventory Undo(Inventory s, Received e) => s;
        public Inventory Do(Inventory s, Renamed e) => s;
        public Inventory Undo(Inventory s, Renamed e) => s;
        public void Dispose() => _probe.Disposed++;
    }
}
