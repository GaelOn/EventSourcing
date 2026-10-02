using EventSourcing.ObjectsAggregation;
using System.Text.Json;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class ReplayDefinitionTests
{
    [Fact]
    public async Task A_reopened_scenario_restores_its_definition_and_history_from_the_same_document()
    {
        var directory = Path.Combine(Path.GetTempPath(), "replay-definition-" + Guid.NewGuid());
        var path = Path.Combine(directory, "scenario.json");
        try
        {
            var definition = new ReplayDefinition<Balance>(new Balance(100m), "balance/v1");
            var live = new EventSourcedAggregate<Balance>(StreamTests.StreamId,
                definition.OpeningState, "balance/v1", StreamTests.Reduce)
                .Apply(new LedgerEvent(StreamTests.FirstId, 25m))
                .Apply(new LedgerEvent(StreamTests.SecondId, -10m));
            var scenario = new StoredScenario(live.Id, definition, live.Events.Cast<LedgerEvent>().ToArray());
            await new JsonFileStore(path).UpdateAsync(_ => JsonSerializer.Serialize(scenario));

            var json = await new JsonFileStore(path).ReadAsync();
            var restored = JsonSerializer.Deserialize<StoredScenario>(json!)!;
            var replayed = EventSourcedAggregate<Balance>.Replay(restored.StreamId,
                restored.Definition, "balance/v1", StreamTests.Reduce, restored.Events);

            Assert.Equal(115m, replayed.State.Amount);
            Assert.Equal(live.State, replayed.State);
            Assert.Equal(2, replayed.PersistedVersion);
            Assert.Empty(replayed.PendingEvents);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Persisted_opening_inputs_and_rule_version_restore_the_original_result()
    {
        var definition = new ReplayDefinition<Balance>(new Balance(100m), "balance/v1");
        var live = StreamTests.Create().Apply(new LedgerEvent(StreamTests.FirstId, 25m));
        var storedDefinition = JsonSerializer.Serialize(definition);
        var restored = JsonSerializer.Deserialize<ReplayDefinition<Balance>>(storedDefinition)!;

        var replayed = EventSourcedAggregate<Balance>.Replay(StreamTests.StreamId,
            restored, "balance/v1", StreamTests.Reduce, live.Events);

        Assert.Equal(live.State, replayed.State);
        Assert.Empty(replayed.PendingEvents);
    }

    [Fact]
    public void A_different_rule_version_is_rejected_before_reducing_any_history()
    {
        var definition = new ReplayDefinition<Balance>(new Balance(100m), "balance/v1");
        var history = StreamTests.Create().Apply(new LedgerEvent(StreamTests.FirstId, 25m)).Events;
        var calls = 0;

        Assert.Throws<ReplayRuleVersionException>(() => EventSourcedAggregate<Balance>.Replay(
            StreamTests.StreamId, definition, "balance/v2",
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, history));

        Assert.Equal(0, calls);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Replay_requires_an_identifiable_rule_version(string version)
    {
        Assert.Throws<ArgumentException>(() => new ReplayDefinition<Balance>(new Balance(100m), version));
    }

    private sealed record StoredScenario(Guid StreamId, ReplayDefinition<Balance> Definition, LedgerEvent[] Events);
}
