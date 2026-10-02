using System.Text.Json;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

/// <summary>Test-only atomic ledger with stable event discriminators and explicit payload schemas.</summary>
internal sealed class CatalogJsonStore(string path) : IAtomicEventStreamStore
{
    private readonly JsonFileStore _file = new(path);

    public async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default) =>
        await (await DecodeAsync(cancellationToken)).ReadAsync(streamId, cancellationToken);
    public async Task<StreamSlice> ReadFromAsync(Guid streamId, long afterVersion,
        CancellationToken cancellationToken = default) =>
        await (await DecodeAsync(cancellationToken)).ReadFromAsync(streamId, afterVersion, cancellationToken);
    public async Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds,
        CancellationToken cancellationToken = default) =>
        await (await DecodeAsync(cancellationToken)).ReadBatchAsync(streamIds, cancellationToken);
    public async Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(
        IReadOnlyList<StreamReadRequest> requests, CancellationToken cancellationToken = default) =>
        await (await DecodeAsync(cancellationToken)).ReadBatchFromAsync(requests, cancellationToken);
    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<IEvent> events,
        CancellationToken cancellationToken = default) =>
        AppendBatchAsync([new(streamId, expectedVersion, events)], cancellationToken);
    public Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        var batch = appends.ToArray();
        return _file.UpdateAsync(json =>
        {
            var (store, ids) = Decode(json);
            // The memory adapter validates all expected versions and stages the entire batch before publication.
            store.AppendBatchAsync(batch, cancellationToken).GetAwaiter().GetResult();
            var histories = store.ReadBatchAsync(ids.Concat(batch.Select(a => a.StreamId)).Distinct().ToArray(),
                cancellationToken).GetAwaiter().GetResult();
            return JsonSerializer.Serialize(new Document(1, histories.Select(pair =>
                new StreamRecord(pair.Key, pair.Value.Select(Encode).ToArray())).ToArray()));
        }, cancellationToken);
    }
    private async Task<InMemoryEventStreamStore> DecodeAsync(CancellationToken cancellationToken) =>
        Decode(await _file.ReadAsync(cancellationToken)).Store;
    private static (InMemoryEventStreamStore Store, Guid[] Ids) Decode(string? json)
    {
        var store = new InMemoryEventStreamStore();
        if (json is null) return (store, []);
        var document = JsonSerializer.Deserialize<Document>(json) ?? throw new InvalidDataException("Null ledger.");
        if (document.FormatVersion != 1) throw new UnsupportedEventFormatException("Unsupported ledger format.");
        if (document.Streams is null) throw new InvalidDataException("Missing streams.");
        var appends = document.Streams.Select(stream =>
        {
            if (stream is null || stream.Events is null) throw new InvalidDataException("Missing stream history.");
            return new StreamAppend(stream.Id, 0, stream.Events.Select(record => DecodeEvent(stream.Id, record)));
        }).ToArray();
        store.AppendBatchAsync(appends).GetAwaiter().GetResult();
        return (store, appends.Select(a => a.StreamId).ToArray());
    }
    private static CatalogEvent DecodeEvent(Guid streamId, EventRecord record)
    {
        if (record is null) throw new InvalidDataException("Null event record.");
        if (record.PayloadVersion != 1) throw new UnsupportedEventFormatException("Unsupported event payload schema.");
        var metadata = new EventMetadata(record.Version, streamId);
        return record.Kind switch
        {
            "catalog.received" => new ReceivedEvent(record.Id, Payload<ReceiptPayload>(record.Payload).Quantity, metadata),
            "catalog.renamed" => Rename(record, metadata),
            _ => throw new UnsupportedEventFormatException($"Unknown event discriminator '{record.Kind}'.")
        };
    }
    private static RenamedEvent Rename(EventRecord record, EventMetadata metadata)
    {
        var payload = Payload<RenamePayload>(record.Payload);
        return new(record.Id, payload.Previous, payload.Next, metadata);
    }
    private static T Payload<T>(JsonElement payload) where T : class =>
        payload.Deserialize<T>() ?? throw new InvalidDataException("Null event payload.");
    private static EventRecord Encode(IEvent e) => e switch
    {
        ReceivedEvent received => new(e.Id, e.Metadata.Version!.Value, "catalog.received", 1,
            JsonSerializer.SerializeToElement(new ReceiptPayload(received.Quantity))),
        RenamedEvent renamed => new(e.Id, e.Metadata.Version!.Value, "catalog.renamed", 1,
            JsonSerializer.SerializeToElement(new RenamePayload(renamed.Previous, renamed.Next))),
        _ => throw new UnsupportedEventFormatException($"Unmapped concrete event '{e.GetType()}'.")
    };
    private sealed record Document(int FormatVersion, StreamRecord[] Streams);
    private sealed record StreamRecord(Guid Id, EventRecord[] Events);
    private sealed record EventRecord(Guid Id, long Version, string Kind, int PayloadVersion, JsonElement Payload);
    private sealed record ReceiptPayload(int Quantity);
    private sealed record RenamePayload(string Previous, string Next);
}
