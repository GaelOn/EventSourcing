using EventSourcing.ObjectsAggregation;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class MultiStreamTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "multi-stream-" + Guid.NewGuid());
    private static readonly Guid Source = StreamTests.StreamId;
    private static readonly Guid Destination = StreamTests.SecondId;
    private IAtomicEventStreamStore Store(bool file) => file
        ? new JsonLedgerStore(Path.Combine(_directory, "ledger.json"))
        : new InMemoryEventStreamStore();
    private static EventSourcedAggregate<Balance> Open(Guid id, decimal amount) => new(id, new(amount), "balance/v1", StreamTests.Reduce);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Transfer_commits_both_streams_and_reconciles_household_position(bool file)
    {
        var store = Store(file);
        var source = Open(Source, 100m).Apply(new LedgerEvent(StreamTests.FirstId, -25m));
        var destination = Open(Destination, 0m).Apply(new LedgerEvent(StreamTests.FirstId, 25m));

        var saved = await EventSourcedAggregateBatch.SaveAsync(store, new[] { source, destination });
        var history = await store.ReadBatchAsync(new[] { Source, Destination });

        Assert.All(saved, stream => Assert.Empty(stream.PendingEvents));
        Assert.All(history.Values, events => Assert.Single(events));
        Assert.Equal(100m, saved.Sum(stream => stream.State.Amount));
        Assert.Equal(0m, history.Values.SelectMany(events => events).Sum(e => ((LedgerEvent)e).Amount));
        Assert.Single(source.PendingEvents);
        Assert.Single(destination.PendingEvents);
        if (file)
            Assert.Equal(2, (await Store(true).ReadBatchAsync(new[] { Source, Destination })).Values.Sum(e => e.Count));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task One_stale_participant_rejects_the_entire_transfer(bool file)
    {
        var store = Store(file);
        var source = Open(Source, 100m).Apply(new LedgerEvent(StreamTests.FirstId, -25m));
        var destination = Open(Destination, 0m).Apply(new LedgerEvent(StreamTests.FirstId, 25m));
        await Open(Destination, 0m).HandleAsync(new LedgerEvent(Guid.NewGuid(), 10m), store);

        await Assert.ThrowsAsync<StreamConcurrencyException>(() => EventSourcedAggregateBatch.SaveAsync(store, new[] { source, destination }));

        var history = await store.ReadBatchAsync(new[] { Source, Destination });
        Assert.Empty(history[Source]);
        Assert.Equal(10m, ((LedgerEvent)Assert.Single(history[Destination])).Amount);
        Assert.All(new[] { source, destination }, s => Assert.Equal(0, s.PersistedVersion));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Duplicate_in_one_stream_rejects_all_other_appends(bool file)
    {
        var store = Store(file);
        var saved = await Open(Destination, 0m).HandleAsync(new LedgerEvent(StreamTests.FirstId, 10m), store);
        var source = Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), -25m));
        var duplicate = new LedgerEvent(StreamTests.FirstId, 25m, new(2, Destination));

        await Assert.ThrowsAsync<DuplicateEventException>(() => store.AppendBatchAsync(new[]
        {
            new StreamAppend(Source, 0, source.PendingEvents),
            new StreamAppend(Destination, saved.Version, new[] { duplicate })
        }));
        Assert.Empty(await store.ReadAsync(Source));
        Assert.Single(await store.ReadAsync(Destination));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unchanged_participants_are_still_checked_for_staleness(bool file)
    {
        var store = Store(file);
        var unchanged = Open(Source, 100m);
        await unchanged.HandleAsync(new LedgerEvent(Guid.NewGuid(), 1m), store);
        var changed = Open(Destination, 0m).Apply(new LedgerEvent(Guid.NewGuid(), 25m));
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => EventSourcedAggregateBatch.SaveAsync(store, new[] { unchanged, changed }));
        Assert.Empty(await store.ReadAsync(Destination));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancelled_batch_preserves_every_stream(bool file)
    {
        var store = Store(file);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var streams = new[] { Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), -25m)),
            Open(Destination, 0m).Apply(new LedgerEvent(Guid.NewGuid(), 25m)) };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => EventSourcedAggregateBatch.SaveAsync(store, streams, cancellation.Token));
        Assert.All((await store.ReadBatchAsync(new[] { Source, Destination })).Values, Assert.Empty);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Competing_writers_cannot_both_commit_the_same_expected_version(bool file)
    {
        var memory = Store(false);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
        {
            await start.Task;
            try
            {
                await Open(Source, 100m).HandleAsync(new LedgerEvent(Guid.NewGuid(), i + 1m), file ? Store(true) : memory);
                return true;
            }
            catch (StreamConcurrencyException) { return false; }
            catch (IOException) when (file) { return false; }
        })).ToArray();
        start.SetResult();
        var results = await Task.WhenAll(attempts);
        Assert.Single(results, success => success);
        Assert.Single(await (file ? Store(true) : memory).ReadAsync(Source));
    }

    [Fact]
    public async Task Concurrent_branches_do_not_modify_the_shared_original()
    {
        var original = Open(Source, 100m);
        var branches = await Task.WhenAll(Enumerable.Range(1, 32).Select(i => Task.Run(() =>
            original.Apply(new LedgerEvent(Guid.NewGuid(), i)))));
        Assert.Equal(100m, original.State.Amount);
        Assert.Empty(original.Events);
        Assert.All(branches, branch => Assert.Single(branch.Events));
        Assert.Equal(32, branches.Select(branch => branch.State.Amount).Distinct().Count());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Atomic_reads_never_observe_half_a_transfer(bool file)
    {
        var store = Store(file);
        var source = Open(Source, 100m);
        var destination = Open(Destination, 0m);
        using var phase = new Barrier(2);
        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < 50; i++)
            {
                Assert.True(phase.SignalAndWait(TimeSpan.FromSeconds(10)), "Reader did not reach the write phase.");
                var id = Guid.NewGuid();
                var saved = await EventSourcedAggregateBatch.SaveAsync(store, new[] {
                    source.Apply(new LedgerEvent(id, -1m)), destination.Apply(new LedgerEvent(id, 1m)) });
                source = saved[0];
                destination = saved[1];
                Assert.True(phase.SignalAndWait(TimeSpan.FromSeconds(10)), "Reader did not finish its concurrent read.");
            }
        });
        var reader = Task.Run(async () =>
        {
            for (var i = 0; i < 50; i++)
            {
                Assert.True(phase.SignalAndWait(TimeSpan.FromSeconds(10)), "Writer did not reach the read phase.");
                var concurrent = await store.ReadBatchAsync(new[] { Source, Destination });
                Assert.Equal(concurrent[Source].Count, concurrent[Destination].Count);
                Assert.Equal(0m, concurrent.Values.SelectMany(events => events).Sum(e => ((LedgerEvent)e).Amount));
                var suffixes = await store.ReadBatchFromAsync(new[] {
                    new StreamReadRequest(Source, i), new StreamReadRequest(Destination, i) });
                Assert.Equal(suffixes[Source].Version, suffixes[Destination].Version);
                Assert.Equal(suffixes[Source].Events.Count, suffixes[Destination].Events.Count);
                Assert.Equal(0m, suffixes.Values.SelectMany(slice => slice.Events).Sum(e => ((LedgerEvent)e).Amount));
                Assert.True(phase.SignalAndWait(TimeSpan.FromSeconds(10)), "Writer did not finish its commit.");
                // Every round must observe progress, so the test cannot pass by sampling only an empty ledger.
                var committed = await store.ReadBatchAsync(new[] { Source, Destination });
                Assert.Equal(i + 1, committed[Source].Count);
                Assert.Equal(i + 1, committed[Destination].Count);
                Assert.Equal(0m, committed.Values.SelectMany(events => events).Sum(e => ((LedgerEvent)e).Amount));
            }
        });
        await Task.WhenAll(writer, reader);
        Assert.Equal(50, (await store.ReadAsync(Source)).Count);
    }

    [Fact]
    public async Task Duplicate_participants_are_rejected_before_any_write()
    {
        var store = Store(false);
        var stream = Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), 1m));
        await Assert.ThrowsAsync<ArgumentException>(() => EventSourcedAggregateBatch.SaveAsync(store, new[] { stream, stream }));
        Assert.Empty(await store.ReadAsync(Source));
    }

    [Fact]
    public async Task Independent_concurrent_writes_do_not_lose_other_streams()
    {
        var store = Store(false);
        var ids = Enumerable.Range(0, 32).Select(_ => Guid.NewGuid()).ToArray();
        await Task.WhenAll(ids.Select(id => Task.Run(() =>
            Open(id, 0m).HandleAsync(new LedgerEvent(Guid.NewGuid(), 1m), store))));
        var snapshot = await store.ReadBatchAsync(ids);
        Assert.Equal(32, snapshot.Count);
        Assert.All(snapshot.Values, history => Assert.Single(history));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Previously_read_snapshots_remain_unchanged_after_another_commit(bool file)
    {
        var store = Store(file);
        var snapshot = await store.ReadBatchAsync(new[] { Source, Destination });
        await EventSourcedAggregateBatch.SaveAsync(store, new[] {
            Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), -1m)),
            Open(Destination, 0m).Apply(new LedgerEvent(Guid.NewGuid(), 1m)) });
        Assert.All(snapshot.Values, Assert.Empty);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Nonempty_read_snapshots_keep_their_history_after_later_commits(bool file)
    {
        var store = Store(file);
        var saved = await Open(Source, 100m).HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var snapshot = await store.ReadAsync(Source);

        await saved.HandleAsync(new LedgerEvent(StreamTests.SecondId, -10m), store);

        Assert.Equal(StreamTests.FirstId, Assert.Single(snapshot).Id);
        Assert.Equal(2, (await store.ReadAsync(Source)).Count);
        Assert.Equal(125m, saved.State.Amount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Invalid_suffix_does_not_publish_history_or_consume_valid_event_identities(bool file)
    {
        var store = Store(file);
        await Open(Source, 100m).HandleAsync(new LedgerEvent(StreamTests.FirstId, 25m), store);
        var valid = new LedgerEvent(StreamTests.SecondId, -10m, new(2, Source));
        var gap = new LedgerEvent(Guid.NewGuid(), 1m, new(4, Source));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.AppendAsync(Source, 1, new[] { valid, gap }));

        Assert.Equal(StreamTests.FirstId, Assert.Single(await store.ReadAsync(Source)).Id);
        await store.AppendAsync(Source, 1, new[] { valid });
        var history = await store.ReadAsync(Source);
        Assert.Equal(new[] { StreamTests.FirstId, StreamTests.SecondId }, history.Select(e => e.Id));
        Assert.Equal(115m, EventSourcedAggregate<Balance>.Replay(Source,
            new ReplayDefinition<Balance>(new Balance(100m), "balance/v1"), "balance/v1", StreamTests.Reduce, history).State.Amount);
    }

    [Fact]
    public async Task Failed_file_batch_keeps_both_participants_pending_and_the_ledger_unchanged()
    {
        var store = Store(true);
        var saved = await EventSourcedAggregateBatch.SaveAsync(store, new[] {
            Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), -1m)),
            Open(Destination, 0m).Apply(new LedgerEvent(Guid.NewGuid(), 1m)) });
        var file = Path.Combine(_directory, "ledger.json");
        var before = await File.ReadAllBytesAsync(file);
        var pending = new[] { saved[0].Apply(new LedgerEvent(Guid.NewGuid(), -25m)),
            saved[1].Apply(new LedgerEvent(Guid.NewGuid(), 25m)) };
        using (var lease = new FileStream(file + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAsync<IOException>(() => EventSourcedAggregateBatch.SaveAsync(store, pending));
        }
        Assert.Equal(before, await File.ReadAllBytesAsync(file));
        Assert.All(pending, stream => Assert.Single(stream.PendingEvents));
        var retried = await EventSourcedAggregateBatch.SaveAsync(store, pending);
        Assert.All(retried, stream => Assert.Equal(2, stream.PersistedVersion));
        Assert.Equal(100m, retried.Sum(stream => stream.State.Amount));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Store_rejects_duplicate_participants_even_when_called_directly(bool file)
    {
        var store = Store(file);
        var stream = Open(Source, 100m).Apply(new LedgerEvent(Guid.NewGuid(), 1m));
        var append = new StreamAppend(Source, 0, stream.PendingEvents);
        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendBatchAsync(new[] { append, append }));
        Assert.Empty(await store.ReadAsync(Source));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }
}
