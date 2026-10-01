using EventSourcing.Transaction;

namespace EventSourcing.ObjectsAggregation;

public interface IEventSourcedAggregate<TState, TEvent> : IAggregate
    where TEvent : class, IEvent<TEvent>
{
    IReadOnlyCollection<TEvent> Events {get;}
    TState? State {get;}
}
