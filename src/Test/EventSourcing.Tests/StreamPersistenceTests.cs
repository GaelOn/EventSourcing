using EventSourcing.ObjectsAggregation;
using EventSourcing.Streams;
using EventSourcing.Transaction;

namespace EventSourcing.Tests;

public sealed class StreamPersistenceTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "event-sourcing-" + Guid.NewGuid());
    private string LedgerPath => Path.Combine(_directory, "ledger.json");
    private JsonLedgerStore Store() => new(LedgerPath);

    [Fact]
    public async Task Saved_history_survives_reopening_and_replays_the_same_balance()
    {
        var opening = StreamTests.Create();
        var saved = await opening.Apply(new LedgerEvent(StreamTests.FirstId, 25m))
            .Apply(new LedgerEvent(StreamTests.SecondId, -10m)).SaveAsync(Store());
        var history = await Store().ReadAsync(StreamTests.StreamId);
        var replayed = EventSourcedAggregate<Balance>.Replay(StreamTests.StreamId,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", StreamTests.Reduce, history);

        Assert.Equal(115m, replayed.State.Amount);
        Assert.Equal(saved.State, replayed.State);
        Assert.Equal(2, replayed.Version);
        Assert.Empty(saved.PendingEvents);
        Assert.Equal(100m, opening.State.Amount);
        Assert.Throws<DuplicateEventException>(() => replayed.Apply(new LedgerEvent(StreamTests.FirstId, 999m)));
    }

    [Fact]
    public async Task Stale_save_preserves_file_and_the_unsaved_aggregate_for_reconciliation()
    {
        var first = StreamTests.Create().Apply(new LedgerEvent(StreamTests.FirstId, 25m));
        var stale = StreamTests.Create().Apply(new LedgerEvent(StreamTests.SecondId, -10m));
        await first.SaveAsync(Store());
        var before = await File.ReadAllBytesAsync(LedgerPath);

        await Assert.ThrowsAsync<StreamConcurrencyException>(() => stale.SaveAsync(Store()));

        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
        Assert.Equal(90m, stale.State.Amount);
        Assert.Equal(0, stale.PersistedVersion);
        Assert.Single(stale.PendingEvents);
    }

    [Fact]
    public async Task Storage_rejects_duplicate_identity_even_with_a_current_expected_version()
    {
        await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), Store());
        var before = await File.ReadAllBytesAsync(LedgerPath);
        var duplicate = new LedgerEvent(StreamTests.FirstId, 999m, new(2, StreamTests.StreamId));

        await Assert.ThrowsAsync<DuplicateEventException>(() =>
            Store().AppendAsync(StreamTests.StreamId, 1, [duplicate]));

        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
    }

    [Fact]
    public async Task Invalid_batch_does_not_persist_its_valid_prefix()
    {
        await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), Store());
        var before = await File.ReadAllBytesAsync(LedgerPath);
        var valid = new LedgerEvent(StreamTests.SecondId, 1m, new(2, StreamTests.StreamId));
        var duplicate = new LedgerEvent(StreamTests.FirstId, 1m, new(3, StreamTests.StreamId));
        await Assert.ThrowsAsync<DuplicateEventException>(() => Store().AppendAsync(StreamTests.StreamId, 1, [valid, duplicate]));
        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
    }

    [Fact]
    public async Task Failed_storage_commit_preserves_both_stored_history_and_original_state()
    {
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), Store());
        var before = await File.ReadAllBytesAsync(LedgerPath);
        using (var competingWriter = new FileStream(LedgerPath + ".lock", FileMode.OpenOrCreate,
                   FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAsync<IOException>(() =>
                saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), Store()));
        }
        Assert.Equal(125m, saved.State.Amount);
        Assert.Equal(1, saved.Version);
        Assert.Empty(saved.PendingEvents);
        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
        var retried = await saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), Store());
        Assert.Equal(115m, retried.State.Amount);
        Assert.Equal(2, retried.Version);
    }

    [Fact]
    public async Task Cancelled_commit_preserves_pending_events_and_creates_no_ledger()
    {
        var pending = StreamTests.Create().Apply(new LedgerEvent(StreamTests.FirstId, 25m));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.SaveAsync(Store(), cancellation.Token));
        Assert.False(File.Exists(LedgerPath));
        Assert.Single(pending.PendingEvents);
        Assert.Equal(0, pending.PersistedVersion);
    }

    [Fact]
    public async Task Corrupt_history_is_not_overwritten_by_a_new_commit()
    {
        Directory.CreateDirectory(_directory);
        await File.WriteAllTextAsync(LedgerPath, "corrupt ledger");
        await Assert.ThrowsAsync<System.Text.Json.JsonException>(() =>
            StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), Store()));
        Assert.Equal("corrupt ledger", await File.ReadAllTextAsync(LedgerPath));
    }

    [Fact]
    public async Task Resaving_saved_state_does_not_append_events_twice()
    {
        var saved = await StreamTests.Create().HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), Store());
        var before = await File.ReadAllBytesAsync(LedgerPath);
        Assert.Same(saved, await saved.SaveAsync(Store()));
        Assert.Equal(before, await File.ReadAllBytesAsync(LedgerPath));
        Assert.Single(await Store().ReadAsync(StreamTests.StreamId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Storage_rejects_invalid_version_or_stream_metadata_without_writing(bool wrongStream)
    {
        var metadata = wrongStream ? new EventMetadata(1, StreamTests.SecondId)
            : new EventMetadata(2, StreamTests.StreamId);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Store().AppendAsync(
            StreamTests.StreamId, 0, [new LedgerEvent(StreamTests.FirstId, 25m, metadata)]));
        Assert.False(File.Exists(LedgerPath));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true);
    }
}
