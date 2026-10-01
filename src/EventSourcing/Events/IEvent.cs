namespace EventSourcing;

/// <summary>Immutable event data with a caller-supplied identity, independent of stream position.</summary>
public interface IEvent<out TEvent> where TEvent : class, IEvent<TEvent>
{
    Guid Id { get; }
    EventMetadata Metadata { get; }
    TEvent Clone(EventMetadata metadata);
}

public abstract class EventBase<TEvent> : IEvent<TEvent> where TEvent : class, IEvent<TEvent>
{
    public Guid Id { get; }
    public EventMetadata Metadata { get; }

    protected EventBase(Guid id, EventMetadata metadata)
    {
        if (id == Guid.Empty) throw new ArgumentException("An event requires a caller-supplied ID.", nameof(id));
        ArgumentNullException.ThrowIfNull(metadata);
        Id = id;
        Metadata = metadata;
    }

    public abstract TEvent Clone(EventMetadata metadata);
}
