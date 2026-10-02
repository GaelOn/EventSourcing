using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class SnapshotPersistenceTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "snapshot-persistence-" + Guid.NewGuid());
    private string LedgerPath => Path.Combine(_directory, "ledger.json");
    private string SnapshotPath => Path.Combine(_directory, "snapshots.json");
    private (IAtomicEventStreamStore Events, ISnapshotStore<Balance> Snapshots) Stores(bool file) =>
        file ? (new JsonLedgerStore(LedgerPath), new JsonSnapshotStore<Balance>(SnapshotPath))
            : (new InMemoryEventStreamStore(), new InMemorySnapshotStore<Balance>());

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Loading_uses_a_compatible_checkpoint_and_only_reduces_the_committed_suffix(bool file)
    {
        var (events, snapshots) = Stores(file);
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await snapshots.SaveAsync(saved.CaptureSnapshot("balance/v1", 1));
        await saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), events);
        var calls = 0;

        var loaded = await EventSourcedAggregate<Balance>.LoadAsync(saved.Id,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, events, snapshots);

        Assert.Equal(115m, loaded.State.Amount);
        Assert.Equal(2, loaded.Version);
        Assert.Equal(1, calls);
        Assert.Equal(StreamTests.SecondId, Assert.Single(loaded.Events).Id);
        Assert.Throws<DuplicateEventException>(() => loaded.Apply(new LedgerEvent(StreamTests.FirstId, 1m)));
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    public async Task Missing_or_incompatible_checkpoints_replay_from_the_opening_definition(bool file, int incompatible)
    {
        var (events, snapshots) = Stores(file);
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        if (incompatible != 0)
            await snapshots.SaveAsync(new AggregateSnapshot<Balance>(saved.Id, saved.PersistedVersion, saved.State,
                incompatible == 1 ? "balance/v0" : "balance/v1", incompatible == 2 ? 2 : 1));

        var loaded = await EventSourcedAggregate<Balance>.LoadAsync(saved.Id,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            StreamTests.Reduce, events, snapshots);

        Assert.Equal(125m, loaded.State.Amount);
        Assert.Equal(0, loaded.HistoryStartVersion);
        Assert.Single(loaded.Events);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_checkpoint_ahead_of_the_ledger_is_rejected_without_reducing_business_state(bool file)
    {
        var (events, snapshots) = Stores(file);
        await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await snapshots.SaveAsync(new AggregateSnapshot<Balance>(StreamTests.StreamId, 10, new Balance(999m), "balance/v1", 1));
        var calls = 0;

        await Assert.ThrowsAsync<StreamConcurrencyException>(() => EventSourcedAggregate<Balance>.LoadAsync(
            StreamTests.StreamId, new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, events, snapshots));

        Assert.Equal(0, calls);
        Assert.Single(await events.ReadAsync(StreamTests.StreamId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_late_older_checkpoint_cannot_replace_a_newer_one(bool file)
    {
        var (events, snapshots) = Stores(file);
        var first = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        var second = await first.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), events);
        await snapshots.SaveAsync(second.CaptureSnapshot("balance/v1", 1));

        await snapshots.SaveAsync(first.CaptureSnapshot("balance/v1", 1));

        var checkpoint = await snapshots.ReadAsync(first.Id, "balance/v1", 1);
        Assert.Equal(2, checkpoint!.StreamVersion);
        Assert.Equal(115m, checkpoint.State.Amount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancelled_checkpoint_writes_preserve_the_previous_checkpoint_and_event_ledger(bool file)
    {
        var (events, snapshots) = Stores(file);
        var first = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await snapshots.SaveAsync(first.CaptureSnapshot("balance/v1", 1));
        var second = await first.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), events);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            snapshots.SaveAsync(second.CaptureSnapshot("balance/v1", 1), cancellation.Token));

        Assert.Equal(1, (await snapshots.ReadAsync(first.Id, "balance/v1", 1))!.StreamVersion);
        Assert.Equal(2, (await events.ReadAsync(first.Id)).Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stale_event_saves_cannot_publish_a_checkpoint_of_the_rejected_changes(bool file)
    {
        var (events, snapshots) = Stores(file);
        var first = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await snapshots.SaveAsync(first.CaptureSnapshot("balance/v1", 1));
        await first.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), events);
        var stale = first.Apply(new LedgerEvent(Guid.NewGuid(), 50m));

        await Assert.ThrowsAsync<StreamConcurrencyException>(() => stale.SaveAsync(events));
        Assert.Throws<InvalidOperationException>(() => stale.CaptureSnapshot("balance/v1", 1));

        Assert.Single(stale.PendingEvents);
        Assert.Equal(125m, (await snapshots.ReadAsync(first.Id, "balance/v1", 1))!.State.Amount);
        Assert.Equal(2, (await events.ReadAsync(first.Id)).Count);
    }

    [Fact]
    public async Task A_failed_file_checkpoint_write_preserves_the_checkpoint_and_does_not_undo_committed_events()
    {
        var (events, snapshots) = Stores(true);
        var first = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await snapshots.SaveAsync(first.CaptureSnapshot("balance/v1", 1));
        var second = await first.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), events);
        var beforeEvents = await File.ReadAllBytesAsync(LedgerPath);
        var beforeSnapshot = await File.ReadAllBytesAsync(SnapshotPath);
        using (var lease = new FileStream(SnapshotPath + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAsync<IOException>(() => snapshots.SaveAsync(second.CaptureSnapshot("balance/v1", 1)));
        }

        Assert.Equal(beforeEvents, await File.ReadAllBytesAsync(LedgerPath));
        Assert.Equal(beforeSnapshot, await File.ReadAllBytesAsync(SnapshotPath));
        Assert.Equal(115m, second.State.Amount);
        Assert.Empty(second.PendingEvents);
        await snapshots.SaveAsync(second.CaptureSnapshot("balance/v1", 1));
        Assert.Equal(2, (await snapshots.ReadAsync(second.Id, "balance/v1", 1))!.StreamVersion);
    }

    [Fact]
    public async Task Corrupt_checkpoint_storage_is_reported_without_touching_the_authoritative_ledger()
    {
        var (events, snapshots) = Stores(true);
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        var before = await File.ReadAllBytesAsync(LedgerPath);
        await File.WriteAllTextAsync(SnapshotPath, "corrupt checkpoint");

        await Assert.ThrowsAsync<System.Text.Json.JsonException>(() => EventSourcedAggregate<Balance>.LoadAsync(saved.Id,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            StreamTests.Reduce, events, snapshots));

        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
        Assert.Equal(125m, EventSourcedAggregate<Balance>.Replay(saved.Id, new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1",
            StreamTests.Reduce, await events.ReadAsync(saved.Id)).State.Amount);
    }

    [Fact]
    public async Task Incompatible_state_payloads_are_skipped_before_deserializing_the_current_business_type()
    {
        var (events, snapshots) = Stores(true);
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        await new JsonSnapshotStore<string>(SnapshotPath).SaveAsync(
            new AggregateSnapshot<string>(saved.Id, 1, "an older state representation", "balance/v1", 2));

        var loaded = await EventSourcedAggregate<Balance>.LoadAsync(saved.Id,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            StreamTests.Reduce, events, snapshots);

        Assert.Equal(125m, loaded.State.Amount);
        Assert.Equal(0, loaded.HistoryStartVersion);
        Assert.Single(loaded.Events);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Batch_suffix_reads_restore_both_sides_of_a_transfer_and_reconcile_household_position(bool file)
    {
        var (events, snapshots) = Stores(file);
        var id = Guid.NewGuid();
        var source = StreamTests.Create().Apply(new LedgerEvent(id, -25m));
        var destination = new EventSourcedAggregate<Balance>(StreamTests.SecondId, new Balance(0m), "balance/v1", StreamTests.Reduce)
            .Apply(new LedgerEvent(id, 25m));
        var first = await EventSourcedAggregateBatch.SaveAsync(events, new[] { source, destination });
        var sourceSnapshot = first[0].CaptureSnapshot("balance/v1", 1);
        var destinationSnapshot = first[1].CaptureSnapshot("balance/v1", 1);
        await snapshots.SaveAsync(sourceSnapshot);
        await snapshots.SaveAsync(destinationSnapshot);
        var later = Guid.NewGuid();
        await EventSourcedAggregateBatch.SaveAsync(events, new[] {
            first[0].Apply(new LedgerEvent(later, -10m)), first[1].Apply(new LedgerEvent(later, 10m)) });

        var suffixes = await events.ReadBatchFromAsync(new[] {
            new StreamReadRequest(source.Id, sourceSnapshot.StreamVersion),
            new StreamReadRequest(destination.Id, destinationSnapshot.StreamVersion) });
        var restoredSource = EventSourcedAggregate<Balance>.Replay(sourceSnapshot, "balance/v1", 1,
            StreamTests.Reduce, suffixes[source.Id]);
        var restoredDestination = EventSourcedAggregate<Balance>.Replay(destinationSnapshot, "balance/v1", 1,
            StreamTests.Reduce, suffixes[destination.Id]);

        Assert.Equal(65m, restoredSource.State.Amount);
        Assert.Equal(35m, restoredDestination.State.Amount);
        Assert.Equal(100m, restoredSource.State.Amount + restoredDestination.State.Amount);
        Assert.Equal(restoredSource.Version, restoredDestination.Version);
        Assert.Throws<DuplicateEventException>(() => restoredSource.Apply(new LedgerEvent(id, 1m)));
        Assert.Throws<DuplicateEventException>(() => restoredDestination.Apply(new LedgerEvent(id, 1m)));
        Assert.All((await events.ReadBatchAsync(new[] { source.Id, destination.Id })).Values, history => Assert.Equal(2, history.Count));

        var nextTransfer = Guid.NewGuid();
        var pending = new[] {
            restoredSource.Apply(new LedgerEvent(nextTransfer, -5m)),
            restoredDestination.Apply(new LedgerEvent(nextTransfer, 5m)) };
        var persisted = await EventSourcedAggregateBatch.SaveAsync(events, pending);

        Assert.Equal(100m, persisted.Sum(s => s.State.Amount));
        Assert.All(persisted, s => Assert.Equal(3, s.PersistedVersion));
        Assert.All(persisted, s => Assert.Empty(s.PendingEvents));
        Assert.All(persisted, s => Assert.Equal(1, s.HistoryStartVersion));
        Assert.All(pending, s => Assert.Single(s.PendingEvents));
        Assert.All((await events.ReadBatchAsync(new[] { source.Id, destination.Id })).Values, history => Assert.Equal(3, history.Count));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancelled_loading_does_not_reduce_business_state(bool file)
    {
        var (events, snapshots) = Stores(file);
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), events);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var calls = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => EventSourcedAggregate<Balance>.LoadAsync(saved.Id,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, events, snapshots, cancellation.Token));

        Assert.Equal(0, calls);
        Assert.Single(await events.ReadAsync(saved.Id));
    }

    [Fact]
    public async Task Concurrent_checkpoint_writers_keep_the_newest_committed_version()
    {
        var (events, snapshots) = Stores(false);
        var saved = StreamTests.Create();
        var checkpoints = new List<AggregateSnapshot<Balance>>();
        for (var i = 0; i < 32; i++)
        {
            saved = await saved.HandleAsync(new LedgerEvent(Guid.NewGuid(), 1m), events);
            checkpoints.Add(saved.CaptureSnapshot("balance/v1", 1));
        }

        await Task.WhenAll(checkpoints.Select(snapshot => Task.Run(() => snapshots.SaveAsync(snapshot))));

        var newest = await snapshots.ReadAsync(saved.Id, "balance/v1", 1);
        Assert.Equal(32, newest!.StreamVersion);
        Assert.Equal(132m, newest.State.Amount);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true);
    }
}
