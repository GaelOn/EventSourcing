using EventSourcing.Storage.Json;
using EventSourcing.Streams;
using EventSourcing.TransactionCrashHost;

namespace EventSourcing.Tests;

public sealed class StrictRawAppendTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "strict-append-" + Guid.NewGuid());
    private IDisposable? _lease;
    private IAtomicEventStreamStore Open(bool durable)
    {
        if (!durable) return new InMemoryEventStreamStore();
        var store = new JsonEventStreamStore(Path.Combine(_directory, "events.json"), CrashProbe.Codec());
        _lease = store;
        return store;
    }
    private static StreamAppend Append(Guid stream, long version, Guid id, decimal amount) =>
        new(stream, version, new[] { new SyntheticPosting(id, amount, "raw", new(version + 1, stream)) });
    private static async Task<string[]> Facts(IAtomicEventStreamStore store) =>
        (await store.ReadBatchAsync(new[] { CrashProbe.Source, CrashProbe.Destination }))
        .OrderBy(pair => pair.Key).SelectMany(pair => pair.Value.Cast<SyntheticPosting>()
            .Select(e => $"{pair.Key}/{e.Id}/{e.Amount}/{e.Reason}/{e.Metadata}")).ToArray();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Published_single_and_atomic_retries_including_changed_payloads_are_strict(bool durable)
    {
        var store = Open(durable);
        var a = Append(CrashProbe.Source, 0, CrashProbe.SourceEvent, -25m);
        var b = Append(CrashProbe.Destination, 0, CrashProbe.DestinationEvent, 25m);
        await store.AppendAsync(a.StreamId, a.ExpectedVersion, a.Events);
        var before = await Facts(store);
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendAsync(a.StreamId, 0, a.Events));
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendAsync(a.StreamId, 0,
            Append(a.StreamId, 0, CrashProbe.SourceEvent, -999m).Events));
        await Assert.ThrowsAsync<DuplicateEventException>(() => store.AppendAsync(a.StreamId, 1,
            Append(a.StreamId, 1, CrashProbe.SourceEvent, -999m).Events));
        Assert.Equal(before, await Facts(store));
        await store.AppendAsync(b.StreamId, 0, b.Events);
        before = await Facts(store);
        // Independent commits do not prove publication of this atomic batch.
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendBatchAsync(new[] { a, b }));
        Assert.Equal(before, await Facts(store));
        var next = new[] { Append(a.StreamId, 1, Guid.NewGuid(), -5m), Append(b.StreamId, 1, Guid.NewGuid(), 5m) };
        await store.AppendBatchAsync(next);
        before = await Facts(store);
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendBatchAsync(next));
        var changed = next.Select(x => Append(x.StreamId, x.ExpectedVersion, x.Events[0].Id, 999m)).ToArray();
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendBatchAsync(changed));
        Assert.Equal(before, await Facts(store));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Eventless_participant_is_checked_even_after_valid_event_bearing_participant(bool durable)
    {
        var store = Open(durable);
        await store.AppendAsync(CrashProbe.Destination, 0,
            Append(CrashProbe.Destination, 0, CrashProbe.DestinationEvent, 1m).Events);
        var before = await Facts(store);
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => store.AppendBatchAsync(new[] {
            Append(CrashProbe.Source, 0, CrashProbe.SourceEvent, 2m),
            new StreamAppend(CrashProbe.Destination, 0, Array.Empty<IEvent>()) }));
        Assert.Equal(before, await Facts(store));
    }

    [Fact]
    public async Task Lost_raw_acknowledgement_requires_inspection_not_stale_save_success()
    {
        var lost = true;
        using var store = new JsonEventStreamStore(Path.Combine(_directory, "events.json"), CrashProbe.Codec(), point =>
        {
            if (lost && point == JsonPublicationPoint.AfterReplace)
            {
                lost = false;
                throw new IOException("synthetic lost acknowledgement");
            }
        });
        var stream = new EventStream(CrashProbe.Source).Append(new SyntheticPosting(CrashProbe.SourceEvent, 3m, "raw", new(null, null)));
        await Assert.ThrowsAsync<IOException>(() => stream.SaveAsync(store));
        var bytes = File.ReadAllBytes(Path.Combine(_directory, "events.json"));
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => stream.SaveAsync(store));
        Assert.Equal(3m, Assert.IsType<SyntheticPosting>(Assert.Single(await store.ReadAsync(CrashProbe.Source))).Amount);
        Assert.Equal(bytes, File.ReadAllBytes(Path.Combine(_directory, "events.json")));
        Assert.True(stream.HasPendingChanges);
    }
    public void Dispose()
    {
        _lease?.Dispose();
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }
}
