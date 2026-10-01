using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

/// <summary>
/// Business root composing an immutable event stream. Derived roots expose business operations
/// and entities from State, which must contain the complete immutable root/entity state.
/// The stream remains the single source of state and position; no callback lifecycle is imposed.
/// </summary>
public abstract class EventSourcedAggregate<TState, TEvent> : IEventSourcedAggregate<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    /// <summary>Wrap a stream without copying its state or balances.</summary>
    protected EventSourcedAggregate(EventStream<TState, TEvent> stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        Stream = stream;
    }

    /// <summary>Immutable stream view; applying an event returns a separate branch and does not modify this root.</summary>
    public EventStream<TState, TEvent> Stream { get; }
    /// <summary>Caller-supplied identity, independent of stream position.</summary>
    public Guid Id => Stream.Id;
    /// <summary>Complete immutable business state at the current version.</summary>
    public TState State => Stream.State;
    /// <summary>Current stream position, including pending events.</summary>
    public long Version => Stream.Version;
    /// <summary>Last successfully committed stream position.</summary>
    public long PersistedVersion => Stream.PersistedVersion;
    /// <summary>The checkpoint boundary preceding the retained event suffix.</summary>
    public long HistoryStartVersion => Stream.HistoryStartVersion;
    /// <summary>Calculation rule identity; null only for legacy unbound delegate streams.</summary>
    public string? RuleVersion => Stream.RuleVersion;
    /// <summary>Accepted events awaiting persistence.</summary>
    public IReadOnlyList<TEvent> PendingEvents => Stream.PendingEvents;
    /// <summary>Whether the aggregate has accepted changes awaiting persistence.</summary>
    public bool HasPendingChanges => Stream.HasPendingChanges;

    /// <summary>Capture saved business state under its bound calculation rule identity.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(int schemaVersion) => Stream.CaptureSnapshot(schemaVersion);

    /// <summary>Save an optional checkpoint after committing events; derives rule identity from the stream.</summary>
    public Task SaveSnapshotAsync(ISnapshotStore<TState> store, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return store.SaveAsync(CaptureSnapshot(schemaVersion), cancellationToken);
    }
    /// <summary>Immutable ordered history retained after the snapshot boundary.</summary>
    public IReadOnlyList<TEvent> Events => Stream.Events;
    /// <summary>Business entities projected from the complete aggregate state.</summary>
    public abstract IEnumerable<IEntity> Entities { get; }

    /// <summary>Capture only committed state; an explicit rule label must match an already bound calculation.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(string ruleVersion, int schemaVersion) =>
        Stream.CaptureSnapshot(ruleVersion, schemaVersion);

    /// <summary>Save an optional checkpoint separately from event commits; failure never changes this root.</summary>
    public Task SaveSnapshotAsync(ISnapshotStore<TState> store, string ruleVersion, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return store.SaveAsync(CaptureSnapshot(ruleVersion, schemaVersion), cancellationToken);
    }
}
