namespace EventSourcing.Handlers;

/// <summary>Pure, reentrant transitions with immutable inputs and configuration; neither operation may perform I/O.</summary>
public interface IEventHandler<TState, in TEvent> where TState : class where TEvent : class
{
    /// <summary>Apply an event after checking its business preconditions.</summary>
    TState Do(TState state, TEvent @event);
    /// <summary>Reverse an immediate transition. Events must retain previous values where needed. Reject invalid reversal preconditions. Persist cancellation as a new compensating event instead.</summary>
    TState Undo(TState state, TEvent @event);
    /// <summary>Compatibility alias for Do.</summary>
    TState Handle(TState state, TEvent @event) => Do(state, @event);
}
