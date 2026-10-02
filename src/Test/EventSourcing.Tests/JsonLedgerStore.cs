using System.Collections.ObjectModel;
using System.Text.Json;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

/// <summary>Test adapter: all streams in one document, committed under one exclusive file lease.</summary>
internal sealed class JsonLedgerStore(string path) : IAtomicEventStreamStore
{
    private readonly JsonFileStore _file = new(path);

    public async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId,
        CancellationToken cancellationToken = default) =>
        (await ReadBatchAsync(new[] { streamId }, cancellationToken))[streamId];

    public async Task<StreamSlice> ReadFromAsync(Guid streamId, long afterVersion,
        CancellationToken cancellationToken = default) =>
        (await ReadBatchFromAsync(new[] { new StreamReadRequest(streamId, afterVersion) }, cancellationToken))[streamId];

    public async Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(
        IReadOnlyList<StreamReadRequest> requests, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests);
        var participants = requests.ToArray();
        var ids = new HashSet<Guid>();
        foreach (var request in participants)
        {
            ArgumentNullException.ThrowIfNull(request);
            if (!ids.Add(request.StreamId)) throw new ArgumentException("Duplicate stream in read batch.", nameof(requests));
        }
        var streams = Decode(await _file.ReadAsync(cancellationToken));
        var result = new Dictionary<Guid, StreamSlice>();
        foreach (var request in participants)
        {
            var history = streams.GetValueOrDefault(request.StreamId) ?? Array.Empty<LedgerEvent>();
            if (request.AfterVersion > history.Count)
                throw new StreamConcurrencyException(request.AfterVersion, history.Count, request.StreamId);
            var offset = checked((int)request.AfterVersion);
            result.Add(request.StreamId, new(request.StreamId, request.AfterVersion, history.Count,
                history.Skip(offset), history.Take(offset).Select(e => e.Id)));
        }
        return new ReadOnlyDictionary<Guid, StreamSlice>(result);
    }

    public async Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(
        IReadOnlyList<Guid> streamIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(streamIds);
        var ids = streamIds.ToArray();
        if (ids.Any(id => id == Guid.Empty)) throw new ArgumentException("A stream ID is required.", nameof(streamIds));
        var streams = Decode(await _file.ReadAsync(cancellationToken));
        return new ReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>(ids.Distinct().ToDictionary(
            id => id, id => streams.GetValueOrDefault(id) ?? Array.Empty<LedgerEvent>()));
    }

    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<IEvent> events,
        CancellationToken cancellationToken = default) => AppendBatchAsync(
            new[] { new StreamAppend(streamId, expectedVersion, events) }, cancellationToken);

    public Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        cancellationToken.ThrowIfCancellationRequested();
        var batch = appends.ToArray();
        var ids = new HashSet<Guid>();
        foreach (var append in batch)
        {
            ArgumentNullException.ThrowIfNull(append);
            if (!ids.Add(append.StreamId)) throw new ArgumentException("Duplicate stream in batch.", nameof(appends));
        }
        if (batch.Length == 0) return Task.CompletedTask;
        return _file.UpdateAsync(json =>
        {
            var streams = Decode(json);
            foreach (var append in batch)
            {
                var stored = streams.GetValueOrDefault(append.StreamId) ?? Array.Empty<LedgerEvent>();
                if (stored.Count != append.ExpectedVersion)
                    throw new StreamConcurrencyException(append.ExpectedVersion, stored.Count, append.StreamId);
                var combined = stored.Concat(append.Events).ToArray();
                EventHistory.Validate(append.StreamId, combined);
                streams[append.StreamId] = Array.AsReadOnly(combined);
            }
            var records = streams.Select(pair => new StoredStream(pair.Key, pair.Value.Select(e =>
                new StoredEvent(e.Id, ((LedgerEvent)e).Amount, e.Metadata.Version!.Value)).ToArray())).ToArray();
            return JsonSerializer.Serialize(new LedgerDocument(2, records));
        }, cancellationToken);
    }

    private static Dictionary<Guid, IReadOnlyList<IEvent>> Decode(string? json)
    {
        if (json is null) return [];
        var document = JsonSerializer.Deserialize<LedgerDocument>(json)
            ?? throw new InvalidDataException("Ledger document is null.");
        if (document.SchemaVersion != 2 || document.Streams is null)
            throw new InvalidDataException("Ledger schema is invalid.");
        var result = new Dictionary<Guid, IReadOnlyList<IEvent>>();
        foreach (var stream in document.Streams)
        {
            if (stream is null || stream.Events is null)
                throw new InvalidDataException("Ledger stream is null.");
            var history = stream.Events.Select(e => e is null
                ? throw new InvalidDataException("Ledger event is null.")
                : new LedgerEvent(e.Id, e.Amount, new EventMetadata(e.Version, stream.Id))).ToArray();
            EventHistory.Validate(stream.Id, history);
            if (!result.TryAdd(stream.Id, Array.AsReadOnly(history)))
                throw new InvalidDataException("Duplicate stream in ledger.");
        }
        return result;
    }

    private sealed record StoredEvent(Guid Id, decimal Amount, long Version);
    private sealed record StoredStream(Guid Id, StoredEvent[] Events);
    private sealed record LedgerDocument(int SchemaVersion, StoredStream[] Streams);
}
