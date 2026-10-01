using System.Collections.Immutable;

namespace EventSourcing.Handlers;

/// <summary>
/// An immutable exact-runtime-type handler registry usable as an EventStream reducer.
/// Live application and replay must use the handler set corresponding to the recorded rule version.
/// </summary>
public sealed class EventHandlerDispatcher<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    private readonly ImmutableDictionary<Type, Func<TState, TEvent, TState>> _handlers;

    internal EventHandlerDispatcher(ImmutableDictionary<Type, Func<TState, TEvent, TState>> handlers) =>
        _handlers = handlers;

    public static EventHandlerDispatcherBuilder<TState, TEvent> CreateBuilder() => new();

    /// <summary>Dispatch exactly once; unregistered types and null results are errors.</summary>
    public TState Reduce(TState state, TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(@event);
        if (!_handlers.TryGetValue(@event.GetType(), out var handle))
            throw new UnhandledEventException(@event.GetType());
        return handle(state, @event) ?? throw new InvalidOperationException("Event handler returned null state.");
    }
}

/// <summary>
/// Startup registration builder; not thread-safe. Each Build captures a separate immutable registry.
/// Handler instances are retained, so their state and dependencies must obey the handler contract.
/// </summary>
public sealed class EventHandlerDispatcherBuilder<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    private ImmutableDictionary<Type, Func<TState, TEvent, TState>> _handlers =
        ImmutableDictionary<Type, Func<TState, TEvent, TState>>.Empty;

    public EventHandlerDispatcherBuilder<TState, TEvent> Register<THandledEvent>(
        IEventHandler<TState, THandledEvent> handler) where THandledEvent : class, TEvent
    {
        ArgumentNullException.ThrowIfNull(handler);
        var type = typeof(THandledEvent);
        if (_handlers.ContainsKey(type))
            throw new ArgumentException($"A handler is already registered for '{type.FullName}'.", nameof(handler));
        _handlers = _handlers.Add(type, (state, @event) => handler.Handle(state, (THandledEvent)@event));
        return this;
    }

    public EventHandlerDispatcher<TState, TEvent> Build() => new(_handlers);
}
