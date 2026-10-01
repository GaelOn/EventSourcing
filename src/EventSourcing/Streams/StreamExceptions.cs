namespace EventSourcing.Streams;

/// <summary>An event identity was reused within a stream.</summary>
public sealed class DuplicateEventException : InvalidOperationException
{
    /// <summary>The reused event identity.</summary>
    public Guid EventId { get; }
    /// <summary>The affected stream when known.</summary>
    public Guid? StreamId { get; }
    /// <summary>Create a duplicate identity diagnostic.</summary>
    public DuplicateEventException(Guid id, Guid? streamId = null)
        : base($"Event '{id}' is already present in this stream.") { EventId = id; StreamId = streamId; }
}

/// <summary>A committed stream position differs from the caller's expected position.</summary>
public sealed class StreamConcurrencyException : InvalidOperationException
{
    /// <summary>The position expected by the caller.</summary>
    public long ExpectedVersion { get; }
    /// <summary>The observed storage position.</summary>
    public long ActualVersion { get; }
    /// <summary>The affected stream when known.</summary>
    public Guid? StreamId { get; }
    /// <summary>Create a version conflict diagnostic.</summary>
    public StreamConcurrencyException(long expected, long actual, Guid? streamId = null)
        : base($"Expected stream version {expected}, but storage is at version {actual}.")
    { ExpectedVersion = expected; ActualVersion = actual; StreamId = streamId; }
}

/// <summary>An event clone changed the concrete business event type.</summary>
public sealed class EventCloneException : InvalidOperationException
{
    /// <summary>The original event identity.</summary>
    public Guid EventId { get; }
    /// <summary>The stream accepting the event.</summary>
    public Guid StreamId { get; }
    /// <summary>The submitted event's type.</summary>
    public Type OriginalType { get; }
    /// <summary>The incorrect clone's type.</summary>
    public Type ClonedType { get; }
    /// <summary>Create a diagnostic for an invalid concrete type conversion.</summary>
    public EventCloneException(Guid eventId, Guid streamId, Type originalType, Type clonedType)
        : base($"Cloning event '{eventId}' changed its type from '{originalType}' to '{clonedType}'.")
    { EventId = eventId; StreamId = streamId; OriginalType = originalType; ClonedType = clonedType; }
}
