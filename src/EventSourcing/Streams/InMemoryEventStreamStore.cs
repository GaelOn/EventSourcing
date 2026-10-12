using System.Collections.ObjectModel;
using System.Collections.Immutable;

namespace EventSourcing.Streams;

/// <summary>
/// Thread-safe, non-durable store. Share one instance for one ledger; independent instances are separate ledgers.
/// Event payloads must be immutable. Batch reads and writes are serialized by the same gate.
/// </summary>
public sealed class InMemoryEventStreamStore : EventSourcing.Transactions.ITransactionSessionStore
{
    /// <summary>Identity of this non-durable store lifetime.</summary>
    public Guid SessionId { get; } = Guid.NewGuid();
    private readonly object _gate = new();
    private ImmutableDictionary<Guid, StoredStream> _streams = ImmutableDictionary<Guid, StoredStream>.Empty;

    private sealed record StoredStream(ImmutableList<IEvent> History, ImmutableHashSet<Guid> EventIds)
    {
        public static StoredStream Empty { get; } = new(ImmutableList<IEvent>.Empty, ImmutableHashSet<Guid>.Empty);
    }

    /// <summary>Read committed history from a consistent storage view.</summary>
    public async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default)
        => (await ReadBatchAsync(new[] { streamId }, cancellationToken))[streamId];

    /// <summary>Read a committed suffix and earlier identities from one consistent view.</summary>
    public async Task<StreamSlice> ReadFromAsync(Guid streamId, long afterVersion,
        CancellationToken cancellationToken = default) =>
        (await ReadBatchFromAsync(new[] { new StreamReadRequest(streamId, afterVersion) }, cancellationToken))[streamId];

    /// <summary>Read suffixes and earlier identities for all participants from one consistent view.</summary>
    public Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(
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
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = new Dictionary<Guid, StreamSlice>();
            foreach (var request in participants)
            {
                var stored = _streams.GetValueOrDefault(request.StreamId) ?? StoredStream.Empty;
                if (request.AfterVersion > stored.History.Count)
                    throw new StreamConcurrencyException(request.AfterVersion, stored.History.Count, request.StreamId);
                var offset = checked((int)request.AfterVersion);
                result.Add(request.StreamId, new(request.StreamId, request.AfterVersion, stored.History.Count,
                    stored.History.GetRange(offset, stored.History.Count - offset),
                    stored.History.Take(offset).Select(e => e.Id)));
            }
            return Task.FromResult<IReadOnlyDictionary<Guid, StreamSlice>>(
                new ReadOnlyDictionary<Guid, StreamSlice>(result));
        }
    }

    /// <summary>Read all requested streams from one consistent storage view.</summary>
    public Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(streamIds);
        var ids = streamIds.ToArray();
        if (ids.Any(id => id == Guid.Empty)) throw new ArgumentException("A stream ID is required.", nameof(streamIds));
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = ids.Distinct().ToDictionary(id => id,
                id => (IReadOnlyList<IEvent>)(_streams.GetValueOrDefault(id) ?? StoredStream.Empty).History);
            return Task.FromResult<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>>(
                new ReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>(snapshot));
        }
    }

    /// <summary>Atomically validate expected version and identities before appending the complete event batch.</summary>
    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<IEvent> events,
        CancellationToken cancellationToken = default) =>
        AppendBatchAsync(new[] { new StreamAppend(streamId, expectedVersion, events) }, cancellationToken);

    /// <summary>Atomically validate every participant and commit all appends or none.</summary>
    public Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        var batch = appends.ToArray();
        var ids = new HashSet<Guid>();
        foreach (var append in batch)
        {
            ArgumentNullException.ThrowIfNull(append);
            if (!ids.Add(append.StreamId)) throw new ArgumentException("Duplicate stream in batch.", nameof(appends));
        }
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var next = _streams.ToBuilder();
            foreach (var append in batch)
            {
                var stored = _streams.GetValueOrDefault(append.StreamId) ?? StoredStream.Empty;
                if (stored.History.Count != append.ExpectedVersion)
                    throw new StreamConcurrencyException(append.ExpectedVersion, stored.History.Count, append.StreamId);
                var eventIds = stored.EventIds.ToBuilder();
                var version = append.ExpectedVersion;
                // Stored history was validated before publication. Only the new suffix needs validation.
                foreach (var e in append.Events)
                {
                    ArgumentNullException.ThrowIfNull(e);
                    if (e.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
                    if (!eventIds.Add(e.Id)) throw new DuplicateEventException(e.Id, append.StreamId);
                    EventHistory.ValidatePosition(append.StreamId, e, checked(++version));
                }
                next[append.StreamId] = new(stored.History.AddRange(append.Events), eventIds.ToImmutable());
            }
            cancellationToken.ThrowIfCancellationRequested();
            _streams = next.ToImmutable();
        }
        return Task.CompletedTask;
    }
}
