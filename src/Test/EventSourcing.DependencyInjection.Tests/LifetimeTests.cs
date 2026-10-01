using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.Handlers;
using EventSourcing.Streams;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection.Tests;

public sealed class LifetimeTests
{
    [Theory]
    [InlineData(ServiceLifetime.Singleton, ServiceLifetime.Singleton, 1, true)]
    [InlineData(ServiceLifetime.Scoped, ServiceLifetime.Scoped, 2, false)]
    [InlineData(ServiceLifetime.Transient, ServiceLifetime.Transient, 3, false)]
    [InlineData(ServiceLifetime.Transient, ServiceLifetime.Singleton, 1, false)]
    public void Explicit_lifetimes_control_reuse_while_dispatch_retains_handlers(ServiceLifetime dispatcherLifetime,
        ServiceLifetime handlerLifetime, int expectedHandlers, bool sharedAcrossScopes)
    {
        var probe = new ConstructionProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
            h.AddHandler<Received, ProbeHandler>(handlerLifetime), dispatcherLifetime);
        using var provider = services.BuildServiceProvider(Validated());
        using var firstScope = provider.CreateScope();
        using var secondScope = provider.CreateScope();
        var first = firstScope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        var repeat = firstScope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        var second = secondScope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        if (dispatcherLifetime == ServiceLifetime.Transient) Assert.NotSame(first, repeat);
        else Assert.Same(first, repeat);
        if (sharedAcrossScopes) Assert.Same(first, second); else Assert.NotSame(first, second);
        Assert.Equal(expectedHandlers, probe.Count);
        var state = new Inventory(new("Original", 10));
        var e = new Received(Guid.NewGuid(), 3);
        var stream = new EventStream<Inventory, InventoryEvent>(Guid.NewGuid(), state, first).Apply(e);
        var history = stream.Events.ToArray();
        for (var i = 0; i < 3; i++) Assert.Equal(state, first.Undo(first.Do(state, e), e));
        Assert.Equal(expectedHandlers, probe.Count);
        Assert.Equal(history, stream.Events); Assert.Single(stream.PendingEvents); Assert.Equal(1, stream.Version);
    }

    [Fact]
    public void One_multi_interface_transient_handler_is_constructed_once_per_dispatcher()
    {
        var probe = new ConstructionProbe();
        var services = new ServiceCollection(); services.AddSingleton(probe);
        EventHandlerRegistrationBuilder<Inventory, InventoryEvent>? captured = null;
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
        { captured = h; h.AddHandler<Received, MultiHandler>(ServiceLifetime.Transient).AddHandler<Renamed, MultiHandler>(ServiceLifetime.Transient); });
        var registeredCount = services.Count;
        using var provider = services.BuildServiceProvider(Validated());
        using var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        Assert.Equal(1, probe.Count);
        var opening = new Inventory(new("Original", 10));
        InventoryEvent[] events = [new Received(Guid.NewGuid(), 3), new Renamed(Guid.NewGuid(), "Original", "Next")];
        foreach (var e in events) Assert.Equal(opening, dispatcher.Undo(dispatcher.Do(opening, e), e));
        Assert.Equal(1, probe.Count); Assert.Equal(registeredCount, services.Count);
        Assert.Throws<InvalidOperationException>(() => captured!.AddHandler<Unregistered, NoOpHandler>());
    }

    [Fact]
    public void Singleton_dispatcher_cannot_capture_scoped_handler_with_scope_validation()
    {
        var services = new ServiceCollection(); services.AddSingleton(new ConstructionProbe());
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
            h.AddHandler<Received, ProbeHandler>(ServiceLifetime.Scoped), ServiceLifetime.Singleton);
        using var provider = services.BuildServiceProvider(Validated());
        using var scope = provider.CreateScope();
        Assert.Throws<InvalidOperationException>(() => scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1"));
    }

    [Theory]
    [InlineData(ServiceLifetime.Singleton)]
    [InlineData(ServiceLifetime.Transient)]
    public void Singleton_dispatcher_cannot_capture_scoped_dependency(ServiceLifetime handlerLifetime)
    {
        var services = new ServiceCollection(); services.AddScoped<ScopedDependency>();
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
            h.AddHandler<Received, DependentHandler>(handlerLifetime), ServiceLifetime.Singleton);
        var error = Record.Exception(() =>
        {
            using var provider = services.BuildServiceProvider(Validated());
            using var scope = provider.CreateScope();
            scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        });
        Assert.NotNull(error);
        Assert.True(error is InvalidOperationException or AggregateException);
    }

    [Theory]
    [InlineData(ServiceLifetime.Scoped)]
    [InlineData(ServiceLifetime.Transient)]
    public void Escaped_dispatcher_and_stream_reject_transitions_after_scope_disposal(ServiceLifetime dispatcherLifetime)
    {
        var services = new ServiceCollection(); services.AddSingleton(new ConstructionProbe());
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h => h.AddHandler<Received, ProbeHandler>(), dispatcherLifetime);
        using var provider = services.BuildServiceProvider(Validated());
        var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1");
        var opening = new Inventory(new("Original", 10)); var e = new Received(Guid.NewGuid(), 3);
        var stream = new EventStream<Inventory, InventoryEvent>(Guid.NewGuid(), opening, dispatcher).Apply(e);
        scope.Dispose();
        Assert.Throws<ObjectDisposedException>(() => dispatcher.Do(opening, e));
        Assert.Throws<ObjectDisposedException>(() => dispatcher.Undo(stream.State, e));
        Assert.Throws<ObjectDisposedException>(() => stream.Apply(new Received(Guid.NewGuid(), 1)));
        Assert.Equal(1, stream.Version); Assert.Single(stream.PendingEvents);
        Assert.Equal(new Inventory(new Item("Original", 13)), stream.State);
    }

    [Fact]
    public void Singleton_dispatcher_survives_child_scope_but_rejects_use_after_provider_disposal()
    {
        var services = new ServiceCollection(); services.AddSingleton(new ConstructionProbe());
        services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
            h.AddHandler<Received, ProbeHandler>(ServiceLifetime.Singleton), ServiceLifetime.Singleton);
        var provider = services.BuildServiceProvider(Validated());
        var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1"); scope.Dispose();
        var opening = new Inventory(new("Original", 10)); var e = new Received(Guid.NewGuid(), 1);
        Assert.Equal(11, dispatcher.Do(opening, e).OwnedItem.Quantity);
        provider.Dispose();
        Assert.Throws<ObjectDisposedException>(() => dispatcher.Do(opening, e));
    }

    [Fact]
    public void Conflicting_lifetimes_for_one_implementation_fail_without_partial_registration()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("probe/v1", h =>
            h.AddHandler<Received, MultiHandler>(ServiceLifetime.Singleton).AddHandler<Renamed, MultiHandler>(ServiceLifetime.Scoped)));
        Assert.Empty(services);
    }
    private static ServiceProviderOptions Validated() => new() { ValidateScopes = true, ValidateOnBuild = true };
    public sealed class ConstructionProbe { public int Count { get; private set; } public void Constructed() => Count++; }
    public sealed class ProbeHandler : IEventHandler<Inventory, Received>
    {
        public ProbeHandler(ConstructionProbe probe) { probe.Constructed(); }
        public Inventory Do(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity + e.Quantity } };
        public Inventory Undo(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity - e.Quantity } };
    }
    public sealed class MultiHandler : IEventHandler<Inventory, Received>, IEventHandler<Inventory, Renamed>
    {
        public MultiHandler(ConstructionProbe probe) { probe.Constructed(); }
        public Inventory Do(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity + e.Quantity } };
        public Inventory Undo(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity - e.Quantity } };
        public Inventory Do(Inventory s, Renamed e) => s with { OwnedItem = s.OwnedItem with { Name = e.Next } };
        public Inventory Undo(Inventory s, Renamed e) => s with { OwnedItem = s.OwnedItem with { Name = e.Previous } };
    }
    public sealed class NoOpHandler : IEventHandler<Inventory, Unregistered>
    { public Inventory Do(Inventory s, Unregistered e) => s; public Inventory Undo(Inventory s, Unregistered e) => s; }
    public sealed class ScopedDependency;
    public sealed class DependentHandler(ScopedDependency dependency) : IEventHandler<Inventory, Received>
    {
        private readonly ScopedDependency _dependency = dependency;
        public Inventory Do(Inventory s, Received e) { _ = _dependency; return s; }
        public Inventory Undo(Inventory s, Received e) { _ = _dependency; return s; }
    }
}
