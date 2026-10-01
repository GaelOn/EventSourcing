using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class SnapshotTests
{
    [Fact]
    public async Task Snapshot_loading_reduces_only_later_events_and_preserves_the_full_stream_position()
    {
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = saved.CaptureSnapshot("balance/v1", 1);
        await saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), store);
        var suffix = await store.ReadFromAsync(saved.Id, snapshot.StreamVersion);
        var calls = 0;

        var restored = EventStream<Balance, LedgerEvent>.Replay(snapshot, "balance/v1", 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, suffix);

        Assert.Equal(115m, restored.State.Amount);
        Assert.Equal(2, restored.Version);
        Assert.Equal(2, restored.PersistedVersion);
        Assert.Equal(1, restored.HistoryStartVersion);
        Assert.Equal(StreamTests.SecondId, Assert.Single(restored.Events).Id);
        Assert.Empty(restored.PendingEvents);
        Assert.Equal(1, calls);
        Assert.Equal(125m, snapshot.State.Amount);
        var changed = restored.Apply(new LedgerEvent(Guid.NewGuid(), 5m));
        Assert.Equal(3, Assert.Single(changed.PendingEvents).Metadata.Version);
        Assert.Equal(120m, changed.State.Amount);
        Assert.Equal(115m, restored.State.Amount);
    }

    [Fact]
    public async Task A_snapshot_at_the_current_version_needs_no_reduction_and_retains_duplicate_protection()
    {
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = saved.CaptureSnapshot("balance/v1", 1);
        var suffix = await store.ReadFromAsync(saved.Id, snapshot.StreamVersion);
        var calls = 0;

        var restored = EventStream<Balance, LedgerEvent>.Replay(snapshot, "balance/v1", 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, suffix);

        Assert.Empty(restored.Events);
        Assert.Equal(1, restored.Version);
        Assert.Equal(0, calls);
        Assert.Throws<DuplicateEventException>(() => restored.Apply(new LedgerEvent(StreamTests.FirstId, 999m)));
        Assert.Equal(0, calls);
        var changed = restored.Apply(new LedgerEvent(StreamTests.SecondId, -10m));
        Assert.Equal(2, Assert.Single(changed.PendingEvents).Metadata.Version);
        Assert.Equal(115m, changed.State.Amount);
        Assert.Equal(1, calls);
        Assert.Equal(1, restored.Version);
    }

    [Fact]
    public void Pending_changes_cannot_be_captured_as_a_committed_snapshot()
    {
        var pending = StreamTests.Create().Apply(new LedgerEvent(StreamTests.FirstId, 25m));

        Assert.Throws<InvalidOperationException>(() => pending.CaptureSnapshot("balance/v1", 1));

        Assert.Single(pending.PendingEvents);
        Assert.Equal(0, pending.PersistedVersion);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Invalid_suffixes_are_rejected_before_business_reduction(int invalidCase)
    {
        var id = StreamTests.StreamId;
        var firstId = StreamTests.FirstId;
        var suffix = invalidCase switch
        {
            0 => new LedgerEvent(StreamTests.SecondId, 1m, new(3, id)),
            1 => new LedgerEvent(StreamTests.SecondId, 1m, new(2, Guid.NewGuid())),
            2 => new LedgerEvent(firstId, 1m, new(2, id)),
            _ => new LedgerEvent(StreamTests.SecondId, 1m, new(2, id))
        };
        var prefix = invalidCase == 3 ? Array.Empty<Guid>() : new[] { firstId };

        Assert.ThrowsAny<InvalidOperationException>(() =>
            new StreamSlice<LedgerEvent>(id, 1, 2, new[] { suffix }, prefix));
    }

    [Fact]
    public void A_truncated_suffix_cannot_claim_a_later_storage_version()
    {
        Assert.Throws<InvalidOperationException>(() => new StreamSlice<LedgerEvent>(StreamTests.StreamId,
            1, 3, new[] { new LedgerEvent(StreamTests.SecondId, 1m, new(2, StreamTests.StreamId)) },
            new[] { StreamTests.FirstId }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Snapshot_identity_cursor_rules_and_schema_must_match_before_reduction(int invalidCase)
    {
        var id = StreamTests.StreamId;
        var snapshot = new AggregateSnapshot<Balance>(invalidCase == 0 ? Guid.NewGuid() : id,
            invalidCase == 1 ? 0 : 1, new Balance(125m), "balance/v1", 1);
        var suffix = new StreamSlice<LedgerEvent>(id, 1, 2,
            new[] { new LedgerEvent(StreamTests.SecondId, -10m, new(2, id)) }, new[] { StreamTests.FirstId });
        var calls = 0;

        Assert.ThrowsAny<InvalidOperationException>(() => EventStream<Balance, LedgerEvent>.Replay(snapshot,
            invalidCase == 2 ? "balance/v2" : "balance/v1", invalidCase == 3 ? 2 : 1,
            (state, e) => { calls++; return StreamTests.Reduce(state, e); }, suffix));

        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task Changes_after_snapshot_restore_save_at_the_actual_version_and_preserve_old_history()
    {
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = saved.CaptureSnapshot("balance/v1", 1);
        var restored = EventStream<Balance, LedgerEvent>.Replay(snapshot, "balance/v1", 1,
            StreamTests.Reduce, await store.ReadFromAsync(saved.Id, 1));
        var changed = restored.Apply(new LedgerEvent(StreamTests.SecondId, -10m));

        var persisted = await changed.SaveAsync(store);

        Assert.Equal(2, persisted.PersistedVersion);
        Assert.Empty(persisted.PendingEvents);
        Assert.Single(persisted.Events);
        Assert.Equal(1, persisted.HistoryStartVersion);
        Assert.Equal(115m, persisted.State.Amount);
        Assert.Equal(new[] { StreamTests.FirstId, StreamTests.SecondId },
            (await store.ReadAsync(saved.Id)).Select(e => e.Id));
        Assert.Equal(125m, restored.State.Amount);
        Assert.Equal(1, restored.Version);
    }

    [Fact]
    public async Task A_failed_suffix_reducer_leaves_the_checkpoint_and_storage_unchanged()
    {
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = saved.CaptureSnapshot("balance/v1", 1);
        await saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), store);
        var suffix = await store.ReadFromAsync(saved.Id, 1);

        Assert.Throws<InvalidOperationException>(() => EventStream<Balance, LedgerEvent>.Replay(snapshot,
            "balance/v1", 1, (_, _) => throw new InvalidOperationException("Rejected."), suffix));

        Assert.Equal(125m, snapshot.State.Amount);
        Assert.Equal(2, (await store.ReadAsync(saved.Id)).Count);
    }

    [Fact]
    public async Task A_stale_restored_branch_preserves_its_checkpoint_cursor_and_pending_changes()
    {
        var store = new InMemoryEventStreamStore<LedgerEvent>();
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = saved.CaptureSnapshot("balance/v1", 1);
        var restored = EventStream<Balance, LedgerEvent>.Replay(snapshot, "balance/v1", 1,
            StreamTests.Reduce, await store.ReadFromAsync(saved.Id, 1));
        var pending = restored.Apply(new LedgerEvent(StreamTests.SecondId, -10m));
        await saved.HandleAsync(new LedgerEvent(Guid.NewGuid(), 5m), store);

        await Assert.ThrowsAsync<StreamConcurrencyException>(() => pending.SaveAsync(store));

        Assert.Equal(1, pending.HistoryStartVersion);
        Assert.Equal(1, pending.PersistedVersion);
        Assert.Equal(2, pending.Version);
        Assert.Equal(StreamTests.SecondId, Assert.Single(pending.PendingEvents).Id);
        Assert.Equal(115m, pending.State.Amount);
        Assert.Equal(125m, restored.State.Amount);
        Assert.Equal(125m, snapshot.State.Amount);
        Assert.DoesNotContain(await store.ReadAsync(saved.Id), e => e.Id == StreamTests.SecondId);
    }
}
