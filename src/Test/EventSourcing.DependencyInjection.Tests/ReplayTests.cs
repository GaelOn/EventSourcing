using System.Text.Json;
using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.ExternalHandlers.RegistrationFixture;
using EventSourcing.Handlers;
using EventSourcing.Snapshots;
using EventSourcing.Streams;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection.Tests;

public sealed class ReplayTests
{
    [Fact]
    public async Task Requested_rule_versions_remain_independent_and_snapshot_rules_are_checked()
    {
        using var provider = Services().BuildServiceProvider(Validated());
        using var scope = provider.CreateScope();
        var first = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
        var second = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v2");
        Assert.NotSame(first, second);
        Assert.Equal("inventory/v1", first.RuleVersion); Assert.Equal("inventory/v2", second.RuleVersion);
        var opening = new Inventory(new("Original", 10)); var e = new Received(Guid.NewGuid(), 2);
        Assert.Equal(16, first.Do(opening, e).OwnedItem.Quantity);
        Assert.Equal(14, second.Do(opening, e).OwnedItem.Quantity);
        Assert.Throws<InvalidOperationException>(() => scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("missing"));
        var store = new InMemoryEventStreamStore<InventoryEvent>();
        var saved = await new EventStream<Inventory, InventoryEvent>(Guid.NewGuid(), opening, first).HandleAsync(e, store);
        var history = await store.ReadAsync(saved.Id);
        Assert.Equal(saved.State, EventStream<Inventory, InventoryEvent>.Replay(saved.Id,
            new ReplayDefinition<Inventory>(opening, first.RuleVersion), first, history).State);
        Assert.Throws<ReplayRuleVersionException>(() => EventStream<Inventory, InventoryEvent>.Replay(saved.Id,
            new ReplayDefinition<Inventory>(opening, first.RuleVersion), second, history));
        var snapshot = saved.CaptureSnapshot(1);
        var slice = await store.ReadFromAsync(saved.Id, saved.Version);
        Assert.Throws<ReplayRuleVersionException>(() => EventStream<Inventory, InventoryEvent>.Replay(snapshot, 1, second, slice));
        Assert.Throws<SnapshotSchemaVersionException>(() => EventStream<Inventory, InventoryEvent>.Replay(snapshot, 2, first, slice));
        Assert.Equal(saved.State, EventStream<Inventory, InventoryEvent>.Replay(snapshot, 1, first, slice).State);
        Assert.Same(first, scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1"));
    }

    [Fact]
    public async Task Live_reopened_full_replay_and_checkpoint_suffix_agree_on_owned_entities_and_versions()
    {
        var directory = Path.Combine(Path.GetTempPath(), "di-replay-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "events.json"); var snapshotPath = Path.Combine(directory, "snapshot.json");
        var opening = new Inventory(new("Original", 10)); var id = Guid.NewGuid();
        var receiptId = Guid.NewGuid(); Inventory expected; long version;
        try
        {
            using (var provider = Services().BuildServiceProvider(Validated()))
            using (var scope = provider.CreateScope())
            {
                var rules = scope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
                var stream = new EventStream<Inventory, InventoryEvent>(id, opening, rules);
                var store = new InventoryJsonStore(path);
                stream = await stream.Apply(new Received(receiptId, 2))
                    .Apply(new Renamed(Guid.NewGuid(), "Original", "Renamed")).SaveAsync(store);
                await File.WriteAllTextAsync(snapshotPath, JsonSerializer.Serialize(stream.CaptureSnapshot(1)));
                stream = await stream.HandleAsync(new Received(Guid.NewGuid(), 1), store);
                expected = stream.State; version = stream.Version;
            }
            // A new container/scope resolves fresh handlers with the same immutable configuration.
            using var reopenedProvider = Services().BuildServiceProvider(Validated());
            using var reopenedScope = reopenedProvider.CreateScope();
            var reopenedRules = reopenedScope.ServiceProvider.GetEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v1");
            var reopenedStore = new InventoryJsonStore(path);
            var history = await reopenedStore.ReadAsync(id);
            var full = EventStream<Inventory, InventoryEvent>.Replay(id,
                new ReplayDefinition<Inventory>(opening, reopenedRules.RuleVersion), reopenedRules, history);
            var snapshot = JsonSerializer.Deserialize<AggregateSnapshot<Inventory>>(await File.ReadAllTextAsync(snapshotPath))!;
            var restored = EventStream<Inventory, InventoryEvent>.Replay(snapshot, 1, reopenedRules, await reopenedStore.ReadFromAsync(id, snapshot.StreamVersion));
            Assert.Equal(new Inventory(new Item("Renamed", 19)), expected);
            Assert.Equal(expected, full.State); Assert.Equal(expected, restored.State);
            Assert.Equal(version, full.Version); Assert.Equal(version, restored.Version);
            Assert.Equal(3, restored.PersistedVersion); Assert.Equal(2, restored.HistoryStartVersion);
            Assert.Single(restored.Events); Assert.Empty(restored.PendingEvents);
            Assert.Throws<DuplicateEventException>(() => restored.Apply(new Received(receiptId, 1)));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public void External_business_fixture_and_core_do_not_reference_DI_assemblies()
    {
        Assert.DoesNotContain(typeof(ReceiptHandler).Assembly.GetReferencedAssemblies(), a => a.Name!.Contains("DependencyInjection", StringComparison.Ordinal));
        Assert.DoesNotContain(typeof(EventStream<,>).Assembly.GetReferencedAssemblies(), a => a.Name!.Contains("DependencyInjection", StringComparison.Ordinal));
    }
    private static IServiceCollection Services() => new ServiceCollection().AddFamilyHorizonAccounts()
        .AddEventHandlerDispatcher<Inventory, InventoryEvent>("inventory/v2", h => h
            .AddHandler<Received, DoubleReceiptHandler>().AddHandler<Renamed, RenameHandler>());
    private static ServiceProviderOptions Validated() => new() { ValidateScopes = true, ValidateOnBuild = true };
}

// Single-writer fixture: stable discriminators; persistence behavior remains owned by the core/store contract.
internal sealed class InventoryJsonStore(string path) : IEventStreamStore<InventoryEvent>
{
    private sealed record Stored(Guid Id, Guid StreamId, long Version, string Kind, int Quantity, string? Previous, string? Next);
    public async Task<IReadOnlyList<InventoryEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(path)) return Array.Empty<InventoryEvent>();
        var stored = JsonSerializer.Deserialize<Stored[]>(await File.ReadAllTextAsync(path, cancellationToken))
            ?? throw new InvalidDataException("Null history.");
        var events = stored.Select(r =>
        {
            if (r.StreamId != streamId) throw new InvalidDataException("Wrong stream.");
            var metadata = new EventMetadata(r.Version, r.StreamId);
            return r.Kind switch
            {
                "received/v1" => (InventoryEvent)new Received(r.Id, r.Quantity, metadata),
                "renamed/v1" => new Renamed(r.Id, r.Previous!, r.Next!, metadata),
                _ => throw new InvalidDataException("Unsupported event format.")
            };
        }).ToArray();
        EventHistory.Validate(streamId, events); return events;
    }
    public async Task<StreamSlice<InventoryEvent>> ReadFromAsync(Guid streamId, long afterVersion, CancellationToken cancellationToken = default)
    {
        var history = await ReadAsync(streamId, cancellationToken);
        return new(streamId, afterVersion, history.Count, history.Skip(checked((int)afterVersion)), history.Take(checked((int)afterVersion)).Select(e => e.Id));
    }
    public async Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<InventoryEvent> events, CancellationToken cancellationToken = default)
    {
        var history = await ReadAsync(streamId, cancellationToken);
        if (history.Count != expectedVersion) throw new StreamConcurrencyException(expectedVersion, history.Count, streamId);
        var combined = history.Concat(events).ToArray(); EventHistory.Validate(streamId, combined);
        var records = combined.Select(e => e switch
        {
            Received r => new Stored(r.Id, streamId, r.Metadata.Version!.Value, "received/v1", r.Quantity, null, null),
            Renamed r => new Stored(r.Id, streamId, r.Metadata.Version!.Value, "renamed/v1", 0, r.Previous, r.Next),
            _ => throw new InvalidDataException("Unsupported event type.")
        }).ToArray();
        var temp = path + ".tmp";
        try { await File.WriteAllTextAsync(temp, JsonSerializer.Serialize(records), cancellationToken); cancellationToken.ThrowIfCancellationRequested(); File.Move(temp, path, true); }
        finally { File.Delete(temp); }
    }
}
