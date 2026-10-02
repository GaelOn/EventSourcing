using System.Text.Json;
using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class AggregateRefactorTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "aggregate-refactor-" + Guid.NewGuid());
    private string PathName => Path.Combine(_directory, "ledger.json");
    private static HouseholdData Opening => new(new("Checking", 100m), new("Savings", 20m), new(0m, 0m), 0);
    private static DateOnly Date => new(2026, 10, 1);
    private HouseholdRoot Open() => new(Guid.NewGuid(), Opening, HouseholdRoot.Rules());
    private static IEvent[] Events() => [new IncomeReceived(Guid.NewGuid(), 50m, 100m, 0m, Date.AddDays(3)),
        new MoneyTransferred(Guid.NewGuid(), 30m, 150m, 20m, Date),
        new ExpensePaid(Guid.NewGuid(), 10m, 120m, 0m, Date.AddDays(1)), new MonthClosed(Guid.NewGuid(), 0, Date.AddDays(2))];

    [Fact]
    public async Task Unrelated_external_events_preserve_order_and_reconcile_complete_owned_state_after_reopening()
    {
        var root = Open(); var events = Events(); var store = new HouseholdJsonStore(PathName);
        var definition = new ReplayDefinition<HouseholdData>(root.State, root.RuleVersion);
        await store.SaveDefinitionAsync(root.Id, definition);
        var income = root.Apply(events[0]);
        var transfer = income.Apply(events[1]);
        Assert.Equal(income.State.Total, transfer.State.Total);
        var saved = await transfer.SaveAsync(store);
        Assert.IsType<HouseholdRoot>(saved);
        var snapshotPath = Path.Combine(_directory, "snapshots.json");
        await saved.SaveSnapshotAsync(new JsonSnapshotStore<HouseholdData>(snapshotPath), 1);
        var latest = await saved.ApplyBatch(events.Skip(2)).SaveAsync(store);
        var reopened = new HouseholdJsonStore(PathName);
        var history = await reopened.ReadAsync(root.Id);
        var full = EventSourcedAggregate<HouseholdData>.Replay(root.Id, await reopened.ReadDefinitionAsync(root.Id), HouseholdRoot.Rules(), history);
        var restored = new HouseholdRoot(await EventSourcedAggregate<HouseholdData>.LoadAsync(root.Id, definition, 1, HouseholdRoot.Rules(), reopened, new JsonSnapshotStore<HouseholdData>(snapshotPath)));
        Assert.Equal(latest.State, full.State); Assert.Equal(latest.State, restored.State);
        Assert.Equal(160m, latest.State.Total);
        Assert.Equal(Opening.Total + latest.State.Statement.Income - latest.State.Statement.Expense, latest.State.Total);
        Assert.Equal(Opening, root.State); Assert.Equal(0, root.Version);
        Assert.Equal(events.Select(e => e.GetType()), history.Select(e => e.GetType()));
        Assert.Equal(events.Select(e => e.Id), history.Select(e => e.Id));
        Assert.Equal(new long?[] { 1, 2, 3, 4 }, history.Select(e => e.Metadata.Version));
        Assert.Equal(4, restored.Version); Assert.Equal(4, restored.PersistedVersion); Assert.Equal(2, restored.HistoryStartVersion);
        Assert.Equal(2, restored.Entities.Count()); Assert.Empty(restored.PendingEvents);
        Assert.Throws<DuplicateEventException>(() => restored.Apply(events[0]));
        var next = new IncomeReceived(Guid.NewGuid(), 5m, 110m, 50m, Date);
        var nextSaved = await restored.HandleAsync(next, reopened);
        Assert.IsType<HouseholdRoot>(nextSaved); Assert.Equal(165m, nextSaved.State.Total); Assert.Equal(5, nextSaved.Version);
        var fullNext = full.Apply(next);
        Assert.Equal(fullNext.State, nextSaved.State); Assert.Equal(fullNext.Version, nextSaved.Version);
    }

    [Fact]
    public void Compensation_reversal_rejects_an_invalid_causal_reference()
    {
        var invalid = new IncomeCancelled(Guid.NewGuid(), Guid.Empty, 50m);
        var rules = HouseholdRoot.Rules();
        Assert.Throws<InvalidOperationException>(() => rules.Do(Opening, invalid));
        Assert.Throws<InvalidOperationException>(() => rules.Undo(Opening, invalid));
        Assert.Equal(120m, Opening.Total);
    }

    [Fact]
    public void Failed_batch_and_unknown_events_preserve_the_original_complete_pair()
    {
        var root = Open(); var state = root.State; var stream = root.Stream;
        Assert.Throws<UnhandledEventException>(() => root.ApplyBatch([Events()[0], new Received(Guid.NewGuid(), 3)]));
        Assert.Throws<InvalidOperationException>(() => root.ApplyBatch([Events()[0], new ExpensePaid(Guid.NewGuid(), 999m, 150m, 0m, Date)]));
        Assert.Same(state, root.State); Assert.Same(stream, root.Stream); Assert.Empty(root.Events); Assert.Empty(root.PendingEvents);
    }

    [Fact]
    public async Task Immediate_reversal_is_pure_and_persisted_cancellation_appends_compensation()
    {
        var root = Open(); var dispatcher = HouseholdRoot.Rules(); var events = Events(); var state = root.State;
        foreach (var e in events)
        {
            var after = dispatcher.Do(state, e);
            Assert.Equal(state, dispatcher.Undo(after, e));
            Assert.Throws<InvalidOperationException>(() => dispatcher.Undo(state, e));
            state = after;
        }
        Assert.Empty(root.Events);
        var store = new HouseholdJsonStore(PathName);
        var income = (IncomeReceived)events[0];
        var saved = await root.HandleAsync(income, store);
        Assert.Equal(root.State, dispatcher.Undo(saved.State, saved.Events[0]));
        Assert.Single(saved.Events); Assert.Equal(1, saved.Version);
        var cancelled = await saved.HandleAsync(new IncomeCancelled(Guid.NewGuid(), income.Id, income.Amount), store);
        Assert.Equal(root.State, cancelled.State); Assert.Equal(2, cancelled.Version);
        Assert.IsType<IncomeReceived>(cancelled.Events[0]); Assert.IsType<IncomeCancelled>(cancelled.Events[1]);
        Assert.Equal(income.Id, ((IncomeCancelled)cancelled.Events[1]).OriginalId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Different_state_types_commit_atomically_and_restore_from_one_consistent_batch(bool conflict)
    {
        var store = new HouseholdJsonStore(PathName); var household = Open().Apply(Events()[0]);
        var other = new EventSourcedAggregate<Balance>(Guid.NewGuid(), new(0m), "balance/v1", StreamTests.Reduce).Apply(new LedgerEvent(Guid.NewGuid(), 7m));
        if (conflict)
        {
            await new EventStream(other.Id).Append(new LedgerEvent(Guid.NewGuid(), 1m)).SaveAsync(store);
            var before = await File.ReadAllBytesAsync(PathName);
            await Assert.ThrowsAsync<StreamConcurrencyException>(() => EventSourcedAggregateBatch.SaveAsync(store, new IEventSourcedAggregate[] { household, other }));
            Assert.Equal(before, await File.ReadAllBytesAsync(PathName)); Assert.Single(household.PendingEvents); Assert.Single(other.PendingEvents);
            Assert.Empty(await store.ReadAsync(household.Id)); return;
        }
        var saved = await EventSourcedAggregateBatch.SaveAsync(store, new IEventSourcedAggregate[] { household, other });
        var first = Assert.IsType<HouseholdRoot>(saved[0]); var second = Assert.IsType<EventSourcedAggregate<Balance>>(saved[1]);
        var slices = await store.ReadBatchFromAsync([new(first.Id, 0), new(second.Id, 0)]);
        Assert.Equal(first.State, EventSourcedAggregate<HouseholdData>.Replay(first.Id, new(Opening, first.RuleVersion), HouseholdRoot.Rules(), slices[first.Id].Events).State);
        Assert.Equal(second.State, EventSourcedAggregate<Balance>.Replay(second.Id, new(new(0m), second.RuleVersion), new EventReducer<Balance>(second.RuleVersion, StreamTests.Reduce), slices[second.Id].Events).State);
        Assert.Empty(first.PendingEvents); Assert.Empty(second.PendingEvents);
        Assert.Single(household.PendingEvents); Assert.Single(other.PendingEvents);
    }

    [Theory]
    [InlineData("FormatVersion", "2")]
    [InlineData("PayloadVersion", "2")]
    [InlineData("Kind", "\"unknown\"")]
    public async Task Unsupported_serialized_formats_fail_explicitly_without_overwriting_history(string field, string value)
    {
        var root = Open(); var store = new HouseholdJsonStore(PathName);
        await root.HandleAsync(Events()[0], store);
        var json = await File.ReadAllTextAsync(PathName);
        var corrupt = field == "Kind" ? json.Replace("\"Kind\":\"household.income\"", "\"Kind\":" + value, StringComparison.Ordinal)
            : json.Replace("\"" + field + "\":1", "\"" + field + "\":" + value, StringComparison.Ordinal);
        Assert.NotEqual(json, corrupt); await File.WriteAllTextAsync(PathName, corrupt);
        await Assert.ThrowsAsync<UnsupportedEventFormatException>(() => store.ReadAsync(root.Id));
        await Assert.ThrowsAsync<UnsupportedEventFormatException>(() => root.HandleAsync(Events()[0], store));
        Assert.Equal(corrupt, await File.ReadAllTextAsync(PathName));
    }

    [Fact]
    public async Task Commit_does_not_invoke_a_user_root_constructor_or_reducer_after_persistence()
    {
        var calls = 0; var store = new InMemoryEventStreamStore();
        var rules = new EventReducer<Balance>("balance/v1", (s, e) => { calls++; return StreamTests.Reduce(s, e); });
        var root = new ConstructorGuardRoot(Guid.NewGuid(), rules);
        var pending = root.Apply(new LedgerEvent(Guid.NewGuid(), 1m));
        ConstructorGuardRoot.RejectConstruction = true;
        try
        {
            var saved = await pending.SaveAsync(store);
            Assert.IsType<ConstructorGuardRoot>(saved); Assert.Equal(1, calls); Assert.Empty(saved.PendingEvents);
            Assert.Equal(1, saved.PersistedVersion); Assert.Single(await store.ReadAsync(saved.Id));
            Assert.Equal(1m, saved.Apply(new LedgerEvent(Guid.NewGuid(), 0m)).State.Amount);
        }
        finally { ConstructorGuardRoot.RejectConstruction = false; }
    }
    private sealed class ConstructorGuardRoot : EventSourcedAggregate<Balance>
    {
        public static bool RejectConstruction { get; set; }
        public ConstructorGuardRoot(Guid id, IEventReducer<Balance> rules) : base(id, new(0m), rules)
        { if (RejectConstruction) throw new InvalidOperationException("Construction rejected."); }
    }
    public void Dispose() { if (Directory.Exists(_directory)) Directory.Delete(_directory, true); }
}
