using System.Collections.Immutable;
using EventSourcing.Streams;

namespace EventSourcing.Handlers;

/// <summary>
/// An immutable exact-runtime-type handler registry used by aggregate application and replay.
/// Live application and replay must use the handler set corresponding to the recorded rule version.
/// </summary>
public sealed class EventHandlerDispatcher<TState> : IEventReducer<TState>
    where TState : class
{
    private readonly ImmutableDictionary<Type, Transition> _handlers;

    internal sealed record Transition(Func<TState, IEvent, TState> Do, Func<TState, IEvent, TState> Undo);
    internal EventHandlerDispatcher(string ruleVersion, ImmutableDictionary<Type, Transition> handlers)
    {
        RuleVersion = ruleVersion;
        _handlers = handlers;
    }

    /// <inheritdoc />
    public string RuleVersion { get; }

    /// <summary>Create a startup handler registry for an explicitly identified rule set.</summary>
    public static EventHandlerDispatcherBuilder<TState> CreateBuilder(string ruleVersion) => new(ruleVersion);

    /// <summary>Dispatch exactly once; unregistered types and null results are errors.</summary>
    public TState Reduce(TState state, IEvent @event) => Do(state, @event);

    /// <summary>Apply exactly one registered forward transition.</summary>
    public TState Do(TState state, IEvent @event) => Dispatch(state, @event, reverse: false);

    /// <summary>Calculate reversal without changing stream history or pending events.</summary>
    public TState Undo(TState state, IEvent @event) => Dispatch(state, @event, reverse: true);

    private TState Dispatch(TState state, IEvent @event, bool reverse)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(@event);
        if (!_handlers.TryGetValue(@event.GetType(), out var handle))
            throw new UnhandledEventException(@event.GetType());
        return (reverse ? handle.Undo : handle.Do)(state, @event) ?? throw new InvalidOperationException("Event handler returned null state.");
    }
}

/// <summary>
/// Startup registration builder; not thread-safe. Each Build captures a separate immutable registry.
/// Handler instances are retained, so their state and dependencies must obey the handler contract.
/// </summary>
public sealed class EventHandlerDispatcherBuilder<TState>
    where TState : class
{
    private readonly string _ruleVersion;
    private ImmutableDictionary<Type, EventHandlerDispatcher<TState>.Transition> _handlers =
        ImmutableDictionary<Type, EventHandlerDispatcher<TState>.Transition>.Empty;

    /// <summary>Create a startup builder for one identified business rule set.</summary>
    public EventHandlerDispatcherBuilder(string ruleVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        _ruleVersion = ruleVersion;
    }

    /// <summary>Register exactly one pure forward and reverse handler for a concrete event type.</summary>
    public EventHandlerDispatcherBuilder<TState> Register<THandledEvent>(
        IEventHandler<TState, THandledEvent> handler) where THandledEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(handler);
        var type = typeof(THandledEvent);
        if (type.IsAbstract || type.IsInterface)
            throw new ArgumentException("Event registrations require a concrete event class.", nameof(handler));
        if (_handlers.ContainsKey(type))
            throw new ArgumentException($"A handler is already registered for '{type.FullName}'.", nameof(handler));
        _handlers = _handlers.Add(type, new((state, @event) => handler.Do(state, (THandledEvent)@event),
            (state, @event) => handler.Undo(state, (THandledEvent)@event)));
        return this;
    }

    /// <summary>Freeze the current registrations into an immutable dispatcher.</summary>
    public EventHandlerDispatcher<TState> Build() => new(_ruleVersion, _handlers);
}
