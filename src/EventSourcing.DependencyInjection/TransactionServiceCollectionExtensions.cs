using EventSourcing.Transactions;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection;

/// <summary>Optional injectable local coordination, sharing one authoritative store lifetime.</summary>
public static class TransactionServiceCollectionExtensions
{
    /// <summary>Register one store and journal per container; ownership/recovery belong to that store.</summary>
    public static IServiceCollection AddLocalEventTransactions(this IServiceCollection services, Guid journalStreamId)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (journalStreamId == Guid.Empty) throw new ArgumentException("Journal identity is required.", nameof(journalStreamId));
        if (services.Any(d => d.ServiceType == typeof(IEventTransactionCoordinator)))
            throw new ArgumentException("A transaction coordinator is already registered.", nameof(services));
        services.AddSingleton<IEventTransactionCoordinator>(provider => new LocalEventTransactionCoordinator(
            provider.GetRequiredService<ITransactionSessionStore>(), journalStreamId));
        services.AddSingleton<OperationProcessor>();
        return services;
    }
}
