namespace EventSourcing.Streams;

/// <summary>Immutable append request; event payloads must also be immutable.</summary>
public sealed class StreamAppend<TEvent> where TEvent : class, IEvent<TEvent>
{
    /// <summary>Identity of the stream participating in this operation.</summary>
    public Guid StreamId { get; }
    /// <summary>Committed position required before this append can succeed.</summary>
    public long ExpectedVersion { get; }
    /// <summary>Immutable ordered history retained after the snapshot boundary.</summary>
    public IReadOnlyList<TEvent> Events { get; }

    /// <summary>Create a validated append request with an immutable event list.</summary>
    public StreamAppend(Guid streamId, long expectedVersion, IEnumerable<TEvent> events)
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        if (expectedVersion < 0) throw new ArgumentOutOfRangeException(nameof(expectedVersion));
        ArgumentNullException.ThrowIfNull(events);
        StreamId = streamId;
        ExpectedVersion = expectedVersion;
        Events = Array.AsReadOnly(events.ToArray());
    }
}
