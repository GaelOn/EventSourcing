using System.Text.Json.Nodes;
using EventSourcing.Handlers;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class HeterogeneousPersistenceTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "event-sourcing-types-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task Concrete_types_and_state_survive_reopening_full_replay_and_checkpoint_restoration()
    {
        Directory.CreateDirectory(_directory);
        var path = Path.Combine(_directory, "events.json");
        var snapshotPath = Path.Combine(_directory, "snapshot.json");
        var rules = Rules();
        var definition = new ReplayDefinition<CatalogState>(new("Original", 10), rules.RuleVersion);
        var stream = new EventStream<CatalogState, CatalogEvent>(StreamTests.StreamId, definition.OpeningState, rules);
        var store = new CatalogJsonStore(path);
        var saved = await stream.Apply(new ReceivedEvent(StreamTests.FirstId, 5))
            .Apply(new RenamedEvent(StreamTests.SecondId, "Original", "Renamed")).SaveAsync(store);
        await new JsonSnapshotStore<CatalogState>(snapshotPath).SaveAsync(saved.CaptureSnapshot(1));
        var lastId = Guid.Parse("44444444-4444-4444-4444-444444444444");
        saved = await saved.HandleAsync(new ReceivedEvent(lastId, 3), store);

        var reopened = new CatalogJsonStore(path);
        var history = await reopened.ReadAsync(stream.Id);
        Assert.Collection(history, e => Assert.IsType<ReceivedEvent>(e), e => Assert.IsType<RenamedEvent>(e),
            e => Assert.IsType<ReceivedEvent>(e));
        var replayed = EventStream<CatalogState, CatalogEvent>.Replay(stream.Id, definition, rules, history);
        var restored = await EventStream<CatalogState, CatalogEvent>.LoadAsync(stream.Id, definition, 1, rules,
            reopened, new JsonSnapshotStore<CatalogState>(snapshotPath));
        Assert.Equal(new CatalogState("Renamed", 18), saved.State);
        Assert.Equal(saved.State, replayed.State);
        Assert.Equal(saved.State, restored.State);
        Assert.Equal(3, replayed.Version);
        Assert.Equal(3, restored.Version);
        Assert.Equal(3, restored.PersistedVersion);
        Assert.Equal(2, restored.HistoryStartVersion);
        Assert.IsType<ReceivedEvent>(Assert.Single(restored.Events));
        Assert.Throws<DuplicateEventException>(() => restored.Apply(new ReceivedEvent(StreamTests.FirstId, 1)));
        var document = JsonNode.Parse(await File.ReadAllTextAsync(path))!;
        Assert.Equal("catalog.received", document["Streams"]![0]!["Events"]![0]!["Kind"]!.GetValue<string>());
        Assert.Equal(1, document["Streams"]![0]!["Events"]![0]!["PayloadVersion"]!.GetValue<int>());
    }

    [Theory]
    [InlineData("envelope")]
    [InlineData("discriminator")]
    [InlineData("payload")]
    public async Task Unsupported_formats_fail_explicitly_and_failed_appends_preserve_the_document(string format)
    {
        Directory.CreateDirectory(_directory);
        var path = Path.Combine(_directory, "events.json");
        var store = new CatalogJsonStore(path);
        var saved = await new EventStream<CatalogState, CatalogEvent>(StreamTests.StreamId, new("Original", 10), Rules())
            .HandleAsync(new ReceivedEvent(StreamTests.FirstId, 1), store);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(path))!;
        var record = json["Streams"]![0]!["Events"]![0]!;
        if (format == "envelope") json["FormatVersion"] = 999;
        if (format == "discriminator") record["Kind"] = "catalog.unknown";
        if (format == "payload") record["PayloadVersion"] = 999;
        var unsupported = json.ToJsonString();
        await File.WriteAllTextAsync(path, unsupported);
        await Assert.ThrowsAsync<UnsupportedEventFormatException>(() => new CatalogJsonStore(path).ReadAsync(saved.Id));
        var pending = saved.Apply(new ReceivedEvent(StreamTests.SecondId, 1));
        await Assert.ThrowsAsync<UnsupportedEventFormatException>(() => pending.SaveAsync(new CatalogJsonStore(path)));
        Assert.Equal(unsupported, await File.ReadAllTextAsync(path));
        Assert.Single(pending.PendingEvents);
    }

    [Fact]
    public void Both_handlers_reverse_immediate_transitions_and_rename_checks_previous_values()
    {
        var rules = Rules();
        var original = new CatalogState("Original", 10);
        CatalogEvent[] events = [new ReceivedEvent(StreamTests.FirstId, 5),
            new RenamedEvent(StreamTests.SecondId, "Original", "Renamed")];
        foreach (var e in events) Assert.Equal(original, rules.Undo(rules.Do(original, e), e));
        var rename = (RenamedEvent)events[1];
        Assert.Throws<InvalidOperationException>(() => rules.Do(original with { Name = "Other" }, rename));
        Assert.Throws<InvalidOperationException>(() => rules.Undo(original with { Name = "Other" }, rename));
    }

    private static EventHandlerDispatcher<CatalogState, CatalogEvent> Rules() =>
        EventHandlerDispatcher<CatalogState, CatalogEvent>.CreateBuilder("catalog/v1")
            .Register<ReceivedEvent>(new ReceivedHandler()).Register<RenamedEvent>(new RenamedHandler()).Build();
    public void Dispose() { if (Directory.Exists(_directory)) Directory.Delete(_directory, true); }
}

internal sealed record CatalogState(string Name, int Quantity);
internal abstract class CatalogEvent(Guid id, EventMetadata? metadata = null) : EventBase<CatalogEvent>(id, metadata ?? new(null, null));
internal sealed class ReceivedEvent(Guid id, int quantity, EventMetadata? metadata = null) : CatalogEvent(id, metadata)
{
    public int Quantity { get; } = quantity > 0 ? quantity : throw new ArgumentOutOfRangeException(nameof(quantity));
    public override CatalogEvent Clone(EventMetadata metadata) => new ReceivedEvent(Id, Quantity, metadata);
}
internal sealed class RenamedEvent(Guid id, string previous, string next, EventMetadata? metadata = null) : CatalogEvent(id, metadata)
{
    public string Previous { get; } = previous;
    public string Next { get; } = next;
    public override CatalogEvent Clone(EventMetadata metadata) => new RenamedEvent(Id, Previous, Next, metadata);
}
internal sealed class ReceivedHandler : IEventHandler<CatalogState, ReceivedEvent>
{
    public CatalogState Do(CatalogState state, ReceivedEvent e) => state with { Quantity = checked(state.Quantity + e.Quantity) };
    public CatalogState Undo(CatalogState state, ReceivedEvent e) => state.Quantity < e.Quantity
        ? throw new InvalidOperationException("Receipt cannot be reversed after stock was consumed.")
        : state with { Quantity = checked(state.Quantity - e.Quantity) };
}
internal sealed class RenamedHandler : IEventHandler<CatalogState, RenamedEvent>
{
    public CatalogState Do(CatalogState state, RenamedEvent e) => state.Name == e.Previous
        ? state with { Name = e.Next } : throw new InvalidOperationException("Previous name does not match.");
    public CatalogState Undo(CatalogState state, RenamedEvent e) => state.Name == e.Next
        ? state with { Name = e.Previous } : throw new InvalidOperationException("Name changed after this event.");
}
internal sealed class UnsupportedEventFormatException(string reason) : NotSupportedException(reason);
