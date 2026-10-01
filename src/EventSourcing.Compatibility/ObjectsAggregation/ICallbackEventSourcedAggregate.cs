namespace EventSourcing.ObjectsAggregation;

public interface ICallbackEventSourcedAggregate<TState, TEvent> : IAggregate
    where TEvent : class, IEvent<TEvent>
{
    IReadOnlyCollection<TEvent> Events {get;}
    TState? State {get;}
}
