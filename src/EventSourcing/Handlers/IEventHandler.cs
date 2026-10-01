namespace EventSourcing.Handlers;

/// <summary>
/// A pure business state transition. Implementations must not mutate their inputs or perform I/O,
/// and must be reentrant when shared by concurrent streams. Configuration must remain immutable.
/// </summary>
public interface IEventHandler<TState, in TEvent>
    where TState : class
    where TEvent : class
{
    TState Handle(TState state, TEvent @event);
}
