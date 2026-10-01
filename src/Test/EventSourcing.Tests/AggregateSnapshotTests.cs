using System.Collections.Immutable;
using System.Reflection;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class AggregateSnapshotTests
{
    [Fact]
    public async Task A_supported_aggregate_restores_root_information_and_owned_entities_from_a_snapshot()
    {
        Assert.Null(typeof(EventSourcedAggregate<Household, LedgerEvent>).GetCustomAttribute<ObsoleteAttribute>());
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var root = HouseholdAggregate.Create(StreamTests.StreamId, "Family", 100m);
        var saved = await root.Apply(new LedgerEvent(StreamTests.FirstId, 25m)).SaveAsync(store);
        var snapshot = saved.CaptureSnapshot("household/v1", 1);
        var suffix = await store.ReadFromAsync(saved.Id, snapshot.StreamVersion);

        var restored = new HouseholdAggregate(EventStream<Household, LedgerEvent>.Replay(snapshot,
            "household/v1", 1, HouseholdAggregate.Reduce, suffix));

        Assert.Equal("Family", restored.Name);
        Assert.Equal("Checking", Assert.Single(restored.Accounts).Name);
        Assert.Equal("EUR", Assert.Single(restored.Accounts).Currency);
        Assert.Equal(125m, Assert.Single(restored.Accounts).Balance);
        Assert.Same(Assert.Single(restored.Accounts), Assert.Single(restored.Entities));
        Assert.Equal(1, restored.Version);
        Assert.Empty(restored.Events);
        Assert.Equal(100m, Assert.Single(root.Accounts).Balance);
    }

    [Fact]
    public async Task Root_and_entity_state_survive_reopening_a_file_backed_checkpoint()
    {
        var directory = Path.Combine(Path.GetTempPath(), "aggregate-snapshot-" + Guid.NewGuid());
        var path = Path.Combine(directory, "snapshots.json");
        try
        {
            var store = new JsonLedgerStore(Path.Combine(directory, "ledger.json"));
            var root = HouseholdAggregate.Create(StreamTests.StreamId, "Family", 100m);
            var saved = await root.Apply(new LedgerEvent(StreamTests.FirstId, 25m)).SaveAsync(store);
            await saved.SaveSnapshotAsync(new JsonSnapshotStore<Household>(path), "household/v1", 1);

            var restoredStream = await EventStream<Household, LedgerEvent>.LoadAsync(saved.Id,
                new ReplayDefinition<Household>(root.State, "household/v1"), "household/v1", 1,
                (_, _) => throw new InvalidOperationException("No old event should be reduced."),
                new JsonLedgerStore(Path.Combine(directory, "ledger.json")), new JsonSnapshotStore<Household>(path));
            var restored = new HouseholdAggregate(restoredStream);

            Assert.Equal("Family", restored.Name);
            Assert.Equal("Checking", Assert.Single(restored.Accounts).Name);
            Assert.Equal("EUR", Assert.Single(restored.Accounts).Currency);
            Assert.Equal(125m, Assert.Single(restored.Accounts).Balance);
            Assert.Equal(1, restored.Version);
            Assert.Empty(restored.Events);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }
}

internal sealed record AccountEntity(string Name, string Currency, decimal Balance) : IEntity;
internal sealed record Household(string Name, ImmutableList<AccountEntity> Accounts);

internal sealed class HouseholdAggregate(EventStream<Household, LedgerEvent> stream)
    : EventSourcedAggregate<Household, LedgerEvent>(stream)
{
    public string Name => State.Name;
    public IReadOnlyList<AccountEntity> Accounts => State.Accounts;
    public override IEnumerable<IEntity> Entities => State.Accounts;

    public static HouseholdAggregate Create(Guid id, string name, decimal balance) => new(
        new EventStream<Household, LedgerEvent>(id,
            new(name, ImmutableList.Create(new AccountEntity("Checking", "EUR", balance))), Reduce));

    public HouseholdAggregate Apply(LedgerEvent e) => new(Stream.Apply(e));

    public async Task<HouseholdAggregate> SaveAsync(IEventStreamStore<LedgerEvent> store) =>
        new(await Stream.SaveAsync(store));

    public static Household Reduce(Household state, LedgerEvent e)
    {
        var checking = state.Accounts[0];
        return state with { Accounts = state.Accounts.SetItem(0, checking with { Balance = checking.Balance + e.Amount }) };
    }
}
