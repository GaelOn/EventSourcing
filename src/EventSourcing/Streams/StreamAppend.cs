namespace EventSourcing.Streams;

/// <summary>Immutable append request; event payloads must also be immutable.</summary>
public sealed class StreamAppend<TEvent> where TEvent : class, IEvent<TEvent>
{
    public Guid StreamId { get; }
    public long ExpectedVersion { get; }
    public IReadOnlyList<TEvent> Events { get; }

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
