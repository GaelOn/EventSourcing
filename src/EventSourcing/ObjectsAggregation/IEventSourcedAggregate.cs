using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

/// <summary>Business aggregate backed by a single immutable event stream.</summary>
public interface IEventSourcedAggregate<TState, TEvent> : IAggregate
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    /// <summary>Complete immutable business state at the current version.</summary>
    TState State { get; }
    /// <summary>Immutable stream view; applying an event returns a separate branch and does not modify this root.</summary>
    EventStream<TState, TEvent> Stream { get; }
    /// <summary>Committed stream position.</summary>
    long PersistedVersion { get; }
    /// <summary>Snapshot boundary preceding retained history.</summary>
    long HistoryStartVersion { get; }
    /// <summary>Calculation rule identity, if bound.</summary>
    string? RuleVersion { get; }
    /// <summary>Accepted changes awaiting persistence.</summary>
    IReadOnlyList<TEvent> PendingEvents { get; }
    /// <summary>Whether pending events exist.</summary>
    bool HasPendingChanges { get; }
}
