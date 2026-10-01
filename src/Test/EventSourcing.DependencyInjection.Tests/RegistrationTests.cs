using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.ExternalHandlers.RegistrationFixture;
using EventSourcing.Handlers;
using EventSourcing.Streams;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public void External_handlers_use_constructor_configuration_and_client_registration_entry_point()
    {
        var services = new ServiceCollection().AddFamilyHorizonAccounts();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions() { ValidateScopes = true, ValidateOnBuild = true });
        using var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
        var opening = new Inventory(new Item("Original", 10));
        var result = dispatcher.Do(opening, new Received(Guid.NewGuid(), 3));
        Assert.Equal(new Inventory(new Item("Original", 19)), result);
        Assert.Equal(opening, dispatcher.Undo(result, new Received(Guid.NewGuid(), 3)));
    }

    [Fact]
    public void Duplicate_events_and_rule_registrations_fail_without_partial_service_changes()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1", h =>
            h.AddHandler<Received, ReceiptHandler>().AddHandler<Received, DoubleReceiptHandler>()));
        Assert.Empty(services);
        services.AddFamilyHorizonAccounts();
        var count = services.Count;
        Assert.Throws<ArgumentException>(() => services.AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1", h =>
            h.AddHandler<Received, DoubleReceiptHandler>()));
        Assert.Equal(count, services.Count);
    }

    [Fact]
    public async Task Missing_handler_fails_before_persistence_without_modifying_original_stream()
    {
        using var provider = new ServiceCollection().AddFamilyHorizonAccounts().BuildServiceProvider(new ServiceProviderOptions() { ValidateScopes = true });
        using var scope = provider.CreateScope();
        var dispatcher = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
        var stream = new EventStream<Inventory, InventoryEvent>(Guid.NewGuid(), new(new("Original", 10)), dispatcher);
        var store = new CountingStore();
        await Assert.ThrowsAsync<UnhandledEventException>(() => stream.HandleAsync(new Unregistered(Guid.NewGuid()), store));
        Assert.Equal(0, store.Writes); Assert.Equal(0, stream.Version); Assert.Empty(stream.PendingEvents);
        Assert.Equal(new Inventory(new Item("Original", 10)), stream.State);
    }
    private sealed class CountingStore : IEventStreamStore<InventoryEvent>
    {
        public int Writes { get; private set; }
        public Task AppendAsync(Guid id, long version, IReadOnlyList<InventoryEvent> events, CancellationToken token = default)
        { Writes++; return Task.CompletedTask; }
        public Task<IReadOnlyList<InventoryEvent>> ReadAsync(Guid id, CancellationToken token = default) => throw new NotSupportedException();
        public Task<StreamSlice<InventoryEvent>> ReadFromAsync(Guid id, long version, CancellationToken token = default) => throw new NotSupportedException();
    }
}
