using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

/// <summary>Business-root lifecycle independent of state and event types.</summary>
public interface IEventSourcedAggregate : IAggregate
{
    /// <summary>History matching this root's business state.</summary>
    EventStream Stream { get; }
    /// <summary>Last committed position.</summary>
    long PersistedVersion { get; }
    /// <summary>Checkpoint preceding retained events.</summary>
    long HistoryStartVersion { get; }
    /// <summary>Bound transition implementation and immutable configuration identity.</summary>
    string RuleVersion { get; }
    /// <summary>Accepted events awaiting persistence.</summary>
    IReadOnlyList<IEvent> PendingEvents { get; }
    /// <summary>Retained ordered history.</summary>
    IReadOnlyList<IEvent> Events { get; }
    /// <summary>Whether accepted events remain uncommitted.</summary>
    bool HasPendingChanges { get; }
}

/// <summary>A business aggregate with one authoritative immutable state.</summary>
public interface IEventSourcedAggregate<TState> : IEventSourcedAggregate where TState : class
{
    /// <summary>Complete root and entity state corresponding to Stream.</summary>
    TState State { get; }
}
