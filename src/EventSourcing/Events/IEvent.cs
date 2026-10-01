using EventSourcing.Transaction;

namespace EventSourcing;

/// <summary>Immutable event data with a caller-supplied identity, independent of stream position.</summary>
public interface IEvent<out TEvent> where TEvent : class, IEvent<TEvent>
{
    Guid Id { get; }
    TransactionnalData TransactionnalData { get; }
    TEvent Clone(TransactionnalData transactionnalData);
}

public abstract class EventBase<TEvent> : IEvent<TEvent> where TEvent : class, IEvent<TEvent>
{
    public Guid Id { get; }
    public TransactionnalData TransactionnalData { get; }

    protected EventBase(Guid id, TransactionnalData transactionnalData)
    {
        if (id == Guid.Empty) throw new ArgumentException("An event requires a caller-supplied ID.", nameof(id));
        ArgumentNullException.ThrowIfNull(transactionnalData);
        Id = id;
        TransactionnalData = transactionnalData;
    }

    public abstract TEvent Clone(TransactionnalData transactionnalData);
}
