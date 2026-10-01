namespace EventSourcing.Streams;

/// <summary>A pure, reentrant calculation associated with an immutable business rule identity.</summary>
public interface IEventReducer<TState, in TEvent> where TState : class where TEvent : class
{
    /// <summary>Stable identity of the implementation and its immutable configuration.</summary>
    string RuleVersion { get; }
    /// <summary>Return new business state without mutation or external effects.</summary>
    TState Reduce(TState state, TEvent @event);
}

/// <summary>Bind a pure delegate to the business rules it implements.</summary>
public sealed class EventReducer<TState, TEvent> : IEventReducer<TState, TEvent>
    where TState : class where TEvent : class
{
    private readonly Func<TState, TEvent, TState> _reduce;
    /// <inheritdoc />
    public string RuleVersion { get; }
    /// <summary>Create a versioned reducer; the caller must identify its implementation honestly.</summary>
    public EventReducer(string ruleVersion, Func<TState, TEvent, TState> reduce)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        ArgumentNullException.ThrowIfNull(reduce);
        RuleVersion = ruleVersion;
        _reduce = reduce;
    }
    /// <inheritdoc />
    public TState Reduce(TState state, TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(@event);
        return _reduce(state, @event) ?? throw new InvalidOperationException("Reducer returned null state.");
    }
}
