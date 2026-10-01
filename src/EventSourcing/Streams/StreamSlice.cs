using System.Collections.Immutable;

namespace EventSourcing.Streams;

/// <summary>
/// A committed suffix and the earlier identity index from one consistent storage view.
/// Prefix payloads are omitted; their IDs retain duplicate protection when restoring a snapshot.
/// </summary>
public sealed class StreamSlice<TEvent> where TEvent : class, IEvent<TEvent>
{
    /// <summary>Identity of the stream participating in this operation.</summary>
    public Guid StreamId { get; }
    /// <summary>Exclusive cursor preceding the requested suffix.</summary>
    public long AfterVersion { get; }
    /// <summary>Current stream position, including pending events.</summary>
    public long Version { get; }
    /// <summary>Immutable ordered history retained after the snapshot boundary.</summary>
    public IReadOnlyList<TEvent> Events { get; }
    /// <summary>Earlier event identities retained for duplicate protection across the snapshot boundary.</summary>
    public IReadOnlyCollection<Guid> PriorEventIds { get; }
    internal ImmutableHashSet<Guid> EventIds { get; }

    /// <summary>Validate suffix positions and the complete earlier identity index.</summary>
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
            if (!ids.Add(id)) throw new DuplicateEventException(id, streamId);
        }
        if (ids.Count != afterVersion)
            throw new InvalidOperationException("The earlier identity index must cover every event before the suffix.");
        PriorEventIds = ids.ToImmutable();
        var position = afterVersion;
        foreach (var e in suffix)
        {
            ArgumentNullException.ThrowIfNull(e);
            if (e.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
            if (!ids.Add(e.Id)) throw new DuplicateEventException(e.Id, streamId);
            EventHistory.ValidatePosition(streamId, e, checked(++position));
        }
        StreamId = streamId;
        AfterVersion = afterVersion;
        Version = version;
        Events = suffix;
        EventIds = ids.ToImmutable();
    }
}
