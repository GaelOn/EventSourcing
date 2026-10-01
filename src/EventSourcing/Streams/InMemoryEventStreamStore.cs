using System.Collections.ObjectModel;
using System.Collections.Immutable;

namespace EventSourcing.Streams;

/// <summary>
/// Thread-safe, non-durable store. Share one instance for one ledger; independent instances are separate ledgers.
/// Event payloads must be immutable. Batch reads and writes are serialized by the same gate.
/// </summary>
public sealed class InMemoryEventStreamStore<TEvent> : IAtomicEventStreamStore<TEvent>
    where TEvent : class, IEvent<TEvent>
{
    private readonly object _gate = new();
    private ImmutableDictionary<Guid, StoredStream> _streams = ImmutableDictionary<Guid, StoredStream>.Empty;

    private sealed record StoredStream(ImmutableList<TEvent> History, ImmutableHashSet<Guid> EventIds)
    {
        public static StoredStream Empty { get; } = new(ImmutableList<TEvent>.Empty, ImmutableHashSet<Guid>.Empty);
    }

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
                id => (IReadOnlyList<TEvent>)(_streams.GetValueOrDefault(id) ?? StoredStream.Empty).History);
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
            var next = _streams.ToBuilder();
            foreach (var append in batch)
            {
                var stored = _streams.GetValueOrDefault(append.StreamId) ?? StoredStream.Empty;
                if (stored.History.Count != append.ExpectedVersion)
                    throw new StreamConcurrencyException(append.ExpectedVersion, stored.History.Count);
                var eventIds = stored.EventIds.ToBuilder();
                var version = append.ExpectedVersion;
                // Stored history was validated before publication. Only the new suffix needs validation.
                foreach (var e in append.Events)
                {
                    ArgumentNullException.ThrowIfNull(e);
                    if (e.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
                    if (!eventIds.Add(e.Id)) throw new DuplicateEventException(e.Id);
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
