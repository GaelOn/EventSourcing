using EventSourcing.DependencyInjection;
using EventSourcing.ExternalHandlers.Fixture;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.ExternalHandlers.RegistrationFixture;

// A client-library integration module, separate from the core-only business assembly.
public static class FamilyHorizonAccountRegistration
{
    public static IServiceCollection AddFamilyHorizonAccounts(this IServiceCollection services)
    {
        services.AddEventHandlerDispatcher<Inventory>("inventory/v1", h => h
            .AddHandler<Received, ReceiptHandler>().AddHandler<Renamed, RenameHandler>());
        services.AddSingleton(new ReceiptConfiguration(3));
        return services;
    }
}
