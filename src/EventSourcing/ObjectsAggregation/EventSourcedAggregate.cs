using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

/// <summary>
/// Immutable business root owning the only state value and its matching state-free stream.
/// Derived roots must have immutable fields and project entities from State. Framework updates
/// preserve the runtime root type without invoking subclass constructors or callbacks after commit.
/// </summary>
public class EventSourcedAggregate<TState> : IEventSourcedAggregate<TState>, IPreparedAggregate
    where TState : class
{
    private sealed record Frame(TState State, EventStream Stream, IEventReducer<TState> Rules, string RuleVersion);
    private Frame _frame;

    /// <summary>Create an opening root at version zero with explicitly identified pure rules.</summary>
    public EventSourcedAggregate(Guid id, TState openingState, IEventReducer<TState> reducer)
    {
        ArgumentNullException.ThrowIfNull(openingState);
        ArgumentNullException.ThrowIfNull(reducer);
        ArgumentException.ThrowIfNullOrWhiteSpace(reducer.RuleVersion);
        _frame = new(openingState, new EventStream(id), reducer, reducer.RuleVersion);
    }

    /// <summary>Rewrap only a framework-produced consistent state/history/rule frame.</summary>
    protected EventSourcedAggregate(EventSourcedAggregate<TState> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _frame = source._frame;
    }

    /// <summary>Create an opening root with a pure delegate and its explicit rule identity.</summary>
    public EventSourcedAggregate(Guid id, TState openingState, string ruleVersion, Func<TState, IEvent, TState> reduce)
        : this(id, openingState, new EventReducer<TState>(ruleVersion, reduce)) { }

    /// <inheritdoc />
    public TState State => _frame.State;
    /// <inheritdoc />
    public EventStream Stream => _frame.Stream;
    /// <inheritdoc />
    public Guid Id => Stream.Id;
    /// <inheritdoc />
    public long Version => Stream.Version;
    /// <inheritdoc />
    public long PersistedVersion => Stream.PersistedVersion;
    /// <inheritdoc />
    public long HistoryStartVersion => Stream.HistoryStartVersion;
    /// <inheritdoc />
    public string RuleVersion => _frame.RuleVersion;
    /// <inheritdoc />
    public IReadOnlyList<IEvent> Events => Stream.Events;
    /// <inheritdoc />
    public IReadOnlyList<IEvent> PendingEvents => Stream.PendingEvents;
    /// <inheritdoc />
    public bool HasPendingChanges => Stream.HasPendingChanges;
    /// <summary>Override to project owned entities from the authoritative State.</summary>
    public virtual IEnumerable<IEntity> Entities => Array.Empty<IEntity>();

    private EventSourcedAggregate<TState> With(TState state, EventStream stream)
    {
        var copy = (EventSourcedAggregate<TState>)MemberwiseClone();
        copy._frame = new(state, stream, _frame.Rules, RuleVersion);
        return copy;
    }

    /// <summary>Accept the clone before reduction and publish only a complete matching state/history pair.</summary>
    public EventSourcedAggregate<TState> Apply(IEvent @event)
    {
        var stream = Stream.Append(@event);
        var accepted = stream.PendingEvents[^1];
        var state = _frame.Rules.Reduce(State, accepted) ?? throw new InvalidOperationException("Reducer returned null state.");
        return With(state, stream);
    }

    /// <summary>Apply the complete batch to local immutable branches or fail without modifying this root.</summary>
    public EventSourcedAggregate<TState> ApplyBatch(IEnumerable<IEvent> events)
    {
        ArgumentNullException.ThrowIfNull(events);
        var result = this;
        foreach (var e in events) result = result.Apply(e);
        return result;
    }

    /// <summary>Prepare the matching committed root before I/O; no user callback runs after durable commit.</summary>
    public async Task<EventSourcedAggregate<TState>> SaveAsync(IEventStreamStore store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        if (!HasPendingChanges) return this;
        var saved = With(State, Stream.AsSaved());
        await store.AppendAsync(Id, PersistedVersion, PendingEvents, cancellationToken).ConfigureAwait(false);
        return saved;
    }

    /// <summary>Apply and save, preserving this input root on business or storage failure.</summary>
    public Task<EventSourcedAggregate<TState>> HandleAsync(IEvent @event, IEventStreamStore store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return Apply(@event).SaveAsync(store, cancellationToken);
    }

    IEventSourcedAggregate IPreparedAggregate.PrepareSaved() => !HasPendingChanges ? this : With(State, Stream.AsSaved());

    /// <summary>Capture complete committed state with provenance derived from its bound rules.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(int schemaVersion)
    {
        if (HasPendingChanges) throw new InvalidOperationException("Save pending events before capturing a committed snapshot.");
        return new(Id, PersistedVersion, State, RuleVersion, schemaVersion);
    }

    /// <summary>Capture with a supplied identity only if it matches the bound transition rules.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(string ruleVersion, int schemaVersion)
    {
        CheckRules(RuleVersion, ruleVersion, Id);
        return CaptureSnapshot(schemaVersion);
    }

    /// <summary>Save a checkpoint separately; a checkpoint failure cannot undo committed events.</summary>
    public Task SaveSnapshotAsync(ISnapshotStore<TState> store, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return store.SaveAsync(CaptureSnapshot(schemaVersion), cancellationToken);
    }

    /// <summary>Save a checkpoint after verifying an explicitly supplied rule identity.</summary>
    public Task SaveSnapshotAsync(ISnapshotStore<TState> store, string ruleVersion, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return store.SaveAsync(CaptureSnapshot(ruleVersion, schemaVersion), cancellationToken);
    }

    /// <summary>Validate full history and recorded rule identity before any business reduction.</summary>
    public static EventSourcedAggregate<TState> Replay(Guid id, ReplayDefinition<TState> definition,
        IEventReducer<TState> reducer, IEnumerable<IEvent> history)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(reducer);
        CheckRules(definition.RuleVersion, reducer.RuleVersion, id);
        var stream = EventStream.Restore(id, history);
        var root = new EventSourcedAggregate<TState>(id, definition.OpeningState, reducer);
        var state = definition.OpeningState;
        foreach (var e in stream.Events)
            state = reducer.Reduce(state, e) ?? throw new InvalidOperationException("Reducer returned null state.");
        return root.With(state, stream);
    }

    /// <summary>
    /// Trust the snapshot's business data only at this explicit restoration boundary; verify its identity,
    /// schema, rules and complete suffix. Cursor checks cannot prove arbitrary snapshot data matches history.
    /// </summary>
    public static EventSourcedAggregate<TState> Replay(AggregateSnapshot<TState> snapshot,
        int schemaVersion, IEventReducer<TState> reducer, StreamSlice suffix)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(reducer);
        ArgumentNullException.ThrowIfNull(suffix);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        CheckRules(snapshot.RuleVersion, reducer.RuleVersion, snapshot.AggregateId);
        if (snapshot.SchemaVersion != schemaVersion)
            throw new SnapshotSchemaVersionException(snapshot.SchemaVersion, schemaVersion, snapshot.AggregateId);
        if (snapshot.AggregateId != suffix.StreamId || snapshot.StreamVersion != suffix.AfterVersion)
            throw new InvalidOperationException("Snapshot identity and version must match the stream suffix.");
        var stream = EventStream.Restore(suffix);
        var state = snapshot.State;
        foreach (var e in stream.Events)
            state = reducer.Reduce(state, e) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new EventSourcedAggregate<TState>(snapshot.AggregateId, snapshot.State, reducer).With(state, stream);
    }

    /// <summary>Restore using the schema declared by the snapshot; prefer an explicit expected schema.</summary>
    public static EventSourcedAggregate<TState> Replay(AggregateSnapshot<TState> snapshot,
        IEventReducer<TState> reducer, StreamSlice suffix)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return Replay(snapshot, snapshot.SchemaVersion, reducer, suffix);
    }

    /// <summary>Load the newest compatible checkpoint and suffix or replay the original opening definition.</summary>
    public static async Task<EventSourcedAggregate<TState>> LoadAsync(Guid id, ReplayDefinition<TState> definition,
        int schemaVersion, IEventReducer<TState> reducer, IEventStreamStore store, ISnapshotStore<TState> snapshots,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(reducer);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(snapshots);
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        CheckRules(definition.RuleVersion, reducer.RuleVersion, id);
        cancellationToken.ThrowIfCancellationRequested();
        var snapshot = await snapshots.ReadAsync(id, reducer.RuleVersion, schemaVersion, cancellationToken).ConfigureAwait(false);
        if (snapshot is not null && (snapshot.AggregateId != id || snapshot.RuleVersion != reducer.RuleVersion || snapshot.SchemaVersion != schemaVersion))
            throw new InvalidOperationException("Snapshot storage returned an incompatible checkpoint.");
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = snapshot?.StreamVersion ?? 0;
        var suffix = await store.ReadFromAsync(id, cursor, cancellationToken).ConfigureAwait(false);
        if (suffix.StreamId != id || suffix.AfterVersion != cursor)
            throw new InvalidOperationException("Event storage returned a suffix for a different stream or cursor.");
        cancellationToken.ThrowIfCancellationRequested();
        return snapshot is null ? Replay(id, definition, reducer, suffix.Events) : Replay(snapshot, schemaVersion, reducer, suffix);
    }

    private static void CheckRules(string recorded, string selected, Guid id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selected);
        if (!string.Equals(recorded, selected, StringComparison.Ordinal))
            throw new ReplayRuleVersionException(recorded, selected, id);
    }

    /// <summary>Replay an explicitly identified pure delegate with guarded opening inputs.</summary>
    public static EventSourcedAggregate<TState> Replay(Guid id, ReplayDefinition<TState> definition,
        string ruleVersion, Func<TState, IEvent, TState> reduce, IEnumerable<IEvent> history) =>
        Replay(id, definition, new EventReducer<TState>(ruleVersion, reduce), history);

    /// <summary>Restore a checkpoint using an explicitly identified pure delegate.</summary>
    public static EventSourcedAggregate<TState> Replay(AggregateSnapshot<TState> snapshot,
        string ruleVersion, int schemaVersion, Func<TState, IEvent, TState> reduce, StreamSlice suffix) =>
        Replay(snapshot, schemaVersion, new EventReducer<TState>(ruleVersion, reduce), suffix);

    /// <summary>Load compatible persisted data using an explicitly identified pure delegate.</summary>
    public static Task<EventSourcedAggregate<TState>> LoadAsync(Guid id, ReplayDefinition<TState> definition,
        string ruleVersion, int schemaVersion, Func<TState, IEvent, TState> reduce, IEventStreamStore store,
        ISnapshotStore<TState> snapshots, CancellationToken cancellationToken = default) =>
        LoadAsync(id, definition, schemaVersion, new EventReducer<TState>(ruleVersion, reduce), store, snapshots, cancellationToken);
}

internal interface IPreparedAggregate
{
    IEventSourcedAggregate PrepareSaved();
}
