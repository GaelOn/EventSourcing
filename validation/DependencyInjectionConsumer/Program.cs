using EventSourcing.ObjectsAggregation;
using EventSourcing;
using EventSourcing.DependencyInjection;
using EventSourcing.Handlers;
using EventSourcing.Snapshots;
using EventSourcing.Streams;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection().AddClientRules();
using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
EventSourcedAggregate<Value> saved;
var store = new InMemoryEventStreamStore();
var snapshots = new InMemorySnapshotStore<Value>();
var opening = new Value(10); var id = Guid.NewGuid();
using (var scope = provider.CreateScope())
{
    var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Value>("client/v1");
    var e = new Change(Guid.NewGuid(), 2);
    Require(rules.Undo(rules.Do(opening, e), e) == opening);
    saved = await new EventSourcedAggregate<Value>(id, opening, rules).HandleAsync(e, store);
    Require(saved.State == new Value(16));
    saved = await saved.HandleAsync(new Renamed(Guid.NewGuid(), "Original", "Next"), store);
    Require(saved.State == new Value(16, "Next"));
    await snapshots.SaveAsync(saved.CaptureSnapshot(1));
}
try { saved.Apply(new Change(Guid.NewGuid(), 1)); throw new Exception("Escaped stream accepted."); }
catch (ObjectDisposedException) { }
using (var scope = provider.CreateScope())
{
    var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Value>("client/v1");
    var loaded = await EventSourcedAggregate<Value>.LoadAsync(id, new ReplayDefinition<Value>(opening, rules.RuleVersion), 1, rules, store, snapshots);
    Require(loaded.State == saved.State && loaded.Version == saved.Version);
}
Console.WriteLine("DI package consumer passed: constructor injection, client registration, reversal, replay and scope lifetime.");
static void Require(bool condition) { if (!condition) throw new Exception("Consumer validation failed."); }
sealed record Value(decimal Amount, string Name = "Original");
sealed record ImmutableConfiguration(decimal Multiplier);
sealed class Change(Guid id, decimal amount, EventMetadata? metadata = null) : EventBase<Change>(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public override Change Clone(EventMetadata metadata) => new(Id, Amount, metadata);
}
sealed class ChangeHandler(ImmutableConfiguration configuration) : IEventHandler<Value, Change>
{
    public Value Do(Value state, Change e) => state with { Amount = state.Amount + e.Amount * configuration.Multiplier };
    public Value Undo(Value state, Change e) => state with { Amount = state.Amount - e.Amount * configuration.Multiplier };
}
static class ClientRegistration
{
    public static IServiceCollection AddClientRules(this IServiceCollection services)
    {
        services.AddSingleton(new ImmutableConfiguration(3));
        return services.AddEventHandlerDispatcher<Value>("client/v1", h => h.AddHandler<Change, ChangeHandler>().AddHandler<Renamed, RenameHandler>());
    }
}

sealed class Renamed(Guid id, string previous, string next, EventMetadata? metadata = null) : IEvent
{
    public Guid Id { get; } = id;
    public string Previous { get; } = previous;
    public string Next { get; } = next;
    public EventMetadata Metadata { get; } = metadata ?? new(null, null);
    public IEvent Clone(EventMetadata metadata) => new Renamed(Id, Previous, Next, metadata);
}
sealed class RenameHandler : IEventHandler<Value, Renamed>
{
    public Value Do(Value s, Renamed e) => s.Name == e.Previous ? s with { Name = e.Next } : throw new InvalidOperationException();
    public Value Undo(Value s, Renamed e) => s.Name == e.Next ? s with { Name = e.Previous } : throw new InvalidOperationException();
}
