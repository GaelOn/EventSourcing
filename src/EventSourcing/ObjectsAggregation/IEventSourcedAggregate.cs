using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

public interface IEventSourcedAggregate<TState, TEvent> : IAggregate
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    TState State { get; }
    EventStream<TState, TEvent> Stream { get; }
}
