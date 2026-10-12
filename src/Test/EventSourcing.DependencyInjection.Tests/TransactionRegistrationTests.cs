using EventSourcing.DependencyInjection;
using EventSourcing.Streams;
using EventSourcing.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventSourcing.DependencyInjection.Tests;

public sealed class TransactionRegistrationTests
{
    [Fact]
    public async Task Real_container_shares_store_and_supports_coordinator_substitution()
    {
        var services = new ServiceCollection();
        var store = new InMemoryEventStreamStore(); var journal = Guid.NewGuid();
        services.AddSingleton<ITransactionSessionStore>(store);
        services.AddLocalEventTransactions(journal);
        Assert.Throws<ArgumentException>(() => services.AddLocalEventTransactions(journal));
        var substitute = new ObservedCoordinator(new LocalEventTransactionCoordinator(store, journal));
        services.Replace(ServiceDescriptor.Singleton<IEventTransactionCoordinator>(substitute));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var processor = provider.GetRequiredService<OperationProcessor>();
        var key = new OperationKey("synthetic/v1", "decision");
        Assert.Equal(OperationStatus.Committed, (await processor.ExecuteAsync(key, "v1", _ => Task.FromResult(new OperationPlan(Array.Empty<StreamAppend>(), "none")))).Status);
        Assert.Equal(OperationStatus.AlreadyConsumed, (await processor.ExecuteAsync(key, "v1", _ => throw new Exception("must not run"))).Status);
        Assert.Equal(2, substitute.Begins); Assert.Equal(1, substitute.Commits);
        Assert.Same(substitute, provider.GetRequiredService<IEventTransactionCoordinator>());
    }

    private sealed class ObservedCoordinator(IEventTransactionCoordinator inner) : IEventTransactionCoordinator
    {
        public int Begins { get; private set; }
        public int Commits { get; private set; }
        public Task<OperationResult> BeginAsync(OperationKey key, string version, CancellationToken token = default)
        { Begins++; return inner.BeginAsync(key, version, token); }
        public Task StageAsync(TransactionHandle handle, OperationPlan plan, CancellationToken token = default) => inner.StageAsync(handle, plan, token);
        public Task<OperationResult> CommitAsync(TransactionHandle handle, CancellationToken token = default)
        { Commits++; return inner.CommitAsync(handle, token); }
        public Task RollbackAsync(TransactionHandle handle, CancellationToken token = default) => inner.RollbackAsync(handle, token);
        public Task<OperationResult> ResolveAsync(OperationKey key, string version, TimeSpan wait, CancellationToken token = default) => inner.ResolveAsync(key, version, wait, token);
        public Task<int> RecoverAsync(CancellationToken token = default) => inner.RecoverAsync(token);
    }
}
