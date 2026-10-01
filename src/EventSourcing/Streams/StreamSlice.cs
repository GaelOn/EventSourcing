using System.Collections.Immutable;

namespace EventSourcing.Streams;

/// <summary>
/// A committed suffix and the earlier identity index from one consistent storage view.
/// Prefix payloads are omitted; their IDs retain duplicate protection when restoring a snapshot.
/// </summary>
public sealed class StreamSlice<TEvent> where TEvent : class, IEvent<TEvent>
{
    public Guid StreamId { get; }
    public long AfterVersion { get; }
    public long Version { get; }
    public IReadOnlyList<TEvent> Events { get; }
    public IReadOnlyCollection<Guid> PriorEventIds { get; }
    internal ImmutableHashSet<Guid> EventIds { get; }

    public StreamSlice(Guid streamId, long afterVersion, long version,
        IEnumerable<TEvent> events, IEnumerable<Guid> priorEventIds)
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        if (afterVersion < 0) throw new ArgumentOutOfRangeException(nameof(afterVersion));
        if (version < afterVersion) throw new ArgumentOutOfRangeException(nameof(version));
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(priorEventIds);
        var suffix = events.ToImmutableList();
        if (suffix.Count != version - afterVersion)
            throw new InvalidOperationException("Stream suffix does not reach the reported storage version.");
        var ids = ImmutableHashSet.CreateBuilder<Guid>();
        foreach (var id in priorEventIds)
        {
            if (id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
            if (!ids.Add(id)) throw new DuplicateEventException(id);
        }
        if (ids.Count != afterVersion)
            throw new InvalidOperationException("The earlier identity index must cover every event before the suffix.");
        PriorEventIds = ids.ToImmutable();
        var position = afterVersion;
        foreach (var e in suffix)
        {
            ArgumentNullException.ThrowIfNull(e);
            if (e.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
            if (!ids.Add(e.Id)) throw new DuplicateEventException(e.Id);
            EventHistory.ValidatePosition(streamId, e, checked(++position));
        }
        StreamId = streamId;
        AfterVersion = afterVersion;
        Version = version;
        Events = suffix;
        EventIds = ids.ToImmutable();
    }
}
