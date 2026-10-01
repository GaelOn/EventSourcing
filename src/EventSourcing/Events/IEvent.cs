namespace EventSourcing;

/// <summary>Immutable event data with a caller-supplied identity, independent of stream position.</summary>
public interface IEvent<out TEvent> where TEvent : class, IEvent<TEvent>
{
    /// <summary>Caller-supplied identity, independent of stream position.</summary>
    Guid Id { get; }
    /// <summary>Assigned stream identity and one-based position, or unassigned values before application.</summary>
    EventMetadata Metadata { get; }
    /// <summary>Return an immutable copy preserving concrete type, payload and event identity with the requested metadata.</summary>
    TEvent Clone(EventMetadata metadata);
}

/// <summary>Base for immutable caller-identified events; derived clones must preserve concrete type and payload.</summary>
public abstract class EventBase<TEvent> : IEvent<TEvent> where TEvent : class, IEvent<TEvent>
{
    /// <summary>Caller-supplied identity, independent of stream position.</summary>
    public Guid Id { get; }
    /// <summary>Assigned stream identity and one-based position, or unassigned values before application.</summary>
    public EventMetadata Metadata { get; }

    /// <summary>Validate caller-supplied event identity and metadata.</summary>
    protected EventBase(Guid id, EventMetadata metadata)
    {
        if (id == Guid.Empty) throw new ArgumentException("An event requires a caller-supplied ID.", nameof(id));
        ArgumentNullException.ThrowIfNull(metadata);
        Id = id;
        Metadata = metadata;
    }

    /// <summary>Return an immutable copy preserving concrete type, payload and event identity with the requested metadata.</summary>
    public abstract TEvent Clone(EventMetadata metadata);
}
