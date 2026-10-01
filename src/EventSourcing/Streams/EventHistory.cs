namespace EventSourcing.Streams;

/// <summary>Shared validation for replay and persistence adapters. Order is stream order, not effective date.</summary>
public static class EventHistory
{
    /// <summary>Validate stream order, metadata and unique nonempty event identities before replay.</summary>
    public static void Validate<TEvent>(Guid streamId, IEnumerable<TEvent> events)
        where TEvent : class, IEvent<TEvent>
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        ArgumentNullException.ThrowIfNull(events);
        var ids = new HashSet<Guid>();
        long version = 0;
        foreach (var @event in events)
        {
            ArgumentNullException.ThrowIfNull(@event);
            if (@event.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
            if (!ids.Add(@event.Id)) throw new DuplicateEventException(@event.Id, streamId);
            ValidatePosition(streamId, @event, checked(++version));
        }
    }

    internal static void ValidatePosition<TEvent>(Guid streamId, TEvent @event, long version)
        where TEvent : class, IEvent<TEvent>
    {
        if (@event.Metadata != new EventMetadata(version, streamId))
            throw new InvalidOperationException("Event stream identity or version is inconsistent.");
    }

    /// <summary>Clone an unrecorded event with its stream position, preserving caller identity.</summary>
    public static TEvent AssignMetadata<TEvent>(TEvent @event, Guid streamId, long version)
        where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        if (version < 1) throw new ArgumentOutOfRangeException(nameof(version));
        if (@event.Id == Guid.Empty) throw new ArgumentException("An event ID is required.", nameof(@event));
        if (@event.Metadata != new EventMetadata(null, null))
            throw new InvalidOperationException("Recorded events must be replayed, not accepted again.");
        var metadata = new EventMetadata(version, streamId);
        var tagged = @event.Clone(metadata);
        if (tagged is null || tagged.Id != @event.Id || tagged.Metadata != metadata)
            throw new InvalidOperationException("Cloning must preserve event identity and assign the requested metadata.");
        if (tagged.GetType() != @event.GetType())
            throw new EventCloneException(@event.Id, streamId, @event.GetType(), tagged.GetType());
        return tagged;
    }
}
