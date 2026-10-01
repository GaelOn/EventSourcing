namespace EventSourcing.ObjectsAggregation;

/// <summary>Legacy aggregate projection for synchronous reversible callbacks.</summary>
/// <summary>Legacy callback aggregate contract; immutable business roots use the core aggregate API.</summary>
public interface ICallbackEventSourcedAggregate<TState, TEvent> : IAggregate
    where TEvent : class, IEvent<TEvent>
{
    /// <summary>Accepted history owned by the callback aggregate.</summary>
    /// <summary>Complete history used by the count-based lifecycle.</summary>
    IReadOnlyCollection<TEvent> Events {get;}
    /// <summary>Optional application state.</summary>
    /// <summary>Optional state supplied to the callback root.</summary>
    TState? State {get;}
}
