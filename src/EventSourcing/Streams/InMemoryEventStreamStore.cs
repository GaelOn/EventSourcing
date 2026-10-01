using System.Collections.ObjectModel;

namespace EventSourcing.Streams;

/// <summary>
/// Thread-safe, non-durable store. Share one instance for one ledger; independent instances are separate ledgers.
/// Event payloads must be immutable. Batch reads and writes are serialized by the same gate.
/// </summary>
public sealed class InMemoryEventStreamStore<TEvent> : IAtomicEventStreamStore<TEvent>
    where TEvent : class, IEvent<TEvent>
{
    private readonly object _gate = new();
    private Dictionary<Guid, IReadOnlyList<TEvent>> _streams = [];

    public async Task<IReadOnlyList<TEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default)
        => (await ReadBatchAsync(new[] { streamId }, cancellationToken))[streamId];

    public Task<IReadOnlyDictionary<Guid, IReadOnlyList<TEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(streamIds);
        var ids = streamIds.ToArray();
        if (ids.Any(id => id == Guid.Empty)) throw new ArgumentException("A stream ID is required.", nameof(streamIds));
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = ids.Distinct().ToDictionary(id => id,
                id => _streams.GetValueOrDefault(id) ?? Array.Empty<TEvent>());
            return Task.FromResult<IReadOnlyDictionary<Guid, IReadOnlyList<TEvent>>>(
                new ReadOnlyDictionary<Guid, IReadOnlyList<TEvent>>(snapshot));
        }
    }

    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<TEvent> events,
        CancellationToken cancellationToken = default) =>
        AppendBatchAsync(new[] { new StreamAppend<TEvent>(streamId, expectedVersion, events) }, cancellationToken);

    public Task AppendBatchAsync(IReadOnlyList<StreamAppend<TEvent>> appends,
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
            var next = new Dictionary<Guid, IReadOnlyList<TEvent>>(_streams);
            foreach (var append in batch)
            {
                var stored = _streams.GetValueOrDefault(append.StreamId) ?? Array.Empty<TEvent>();
                if (stored.Count != append.ExpectedVersion)
                    throw new StreamConcurrencyException(append.ExpectedVersion, stored.Count);
                var combined = stored.Concat(append.Events).ToArray();
                EventHistory.Validate(append.StreamId, combined);
                next[append.StreamId] = Array.AsReadOnly(combined);
            }
            cancellationToken.ThrowIfCancellationRequested();
            _streams = next;
        }
        return Task.CompletedTask;
    }
}
