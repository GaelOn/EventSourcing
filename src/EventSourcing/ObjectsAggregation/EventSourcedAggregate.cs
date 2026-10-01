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
    protected EventSourcedAggregate(EventStream<TState, TEvent> stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        Stream = stream;
    }

    public EventStream<TState, TEvent> Stream { get; }
    public Guid Id => Stream.Id;
    public TState State => Stream.State;
    public long Version => Stream.Version;
    public long PersistedVersion => Stream.PersistedVersion;
    public IReadOnlyList<TEvent> Events => Stream.Events;
    public abstract IEnumerable<IEntity> Entities { get; }

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
