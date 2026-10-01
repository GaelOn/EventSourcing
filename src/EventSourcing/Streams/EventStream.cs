using System.Collections.Immutable;
using EventSourcing.Snapshots;

namespace EventSourcing.Streams;

/// <summary>
/// Immutable update mechanics for business state, with a pure reducer shared by live calculation and replay.
/// Instances may be shared between threads when events/state are immutable and the reducer is pure.
/// State and events must be immutable; the reducer must not mutate inputs or perform I/O.
/// </summary>
public sealed class EventStream<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    private readonly Func<TState, TEvent, TState> _reduce;
    private readonly ImmutableList<TEvent> _events;
    private readonly ImmutableList<TEvent> _pendingEvents;
    private readonly ImmutableHashSet<Guid> _eventIds;
    /// <summary>The associated calculation rule identity, or null for a legacy unbound delegate.</summary>
    public string? RuleVersion { get; }
    /// <summary>Whether this view contains changes not yet committed.</summary>
    public bool HasPendingChanges => PendingEvents.Count != 0;
    /// <summary>Caller-supplied identity, independent of stream position.</summary>
    public Guid Id { get; }
    /// <summary>Complete immutable business state at the current version.</summary>
    public TState State { get; }
    /// <summary>Immutable ordered history retained after the snapshot boundary.</summary>
    public IReadOnlyList<TEvent> Events => _events;
    /// <summary>Accepted events awaiting an atomic persistence operation.</summary>
    public IReadOnlyList<TEvent> PendingEvents => _pendingEvents;
    /// <summary>Current stream position, including pending events.</summary>
    public long Version { get; }
    /// <summary>Last successfully committed stream position.</summary>
    public long PersistedVersion { get; }
    /// <summary>Events contains only the history after this snapshot cursor; zero means opening state.</summary>
    public long HistoryStartVersion { get; }

    /// <summary>Create an opening stream using a pure reducer; prefer an identified reducer for persisted scenarios.</summary>
    public EventStream(Guid id, TState openingState, Func<TState, TEvent, TState> reduce)
        : this(id, openingState, reduce, ImmutableList<TEvent>.Empty,
            ImmutableList<TEvent>.Empty, ImmutableHashSet<Guid>.Empty, 0, 0, 0) { }

    /// <summary>Create a stream with a pure calculation and its bound business rule identity.</summary>
    public EventStream(Guid id, TState openingState, IEventReducer<TState, TEvent> reducer)
        : this(id, openingState, (reducer ?? throw new ArgumentNullException(nameof(reducer))).Reduce,
            ImmutableList<TEvent>.Empty, ImmutableList<TEvent>.Empty, ImmutableHashSet<Guid>.Empty,
            0, 0, 0, reducer.RuleVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reducer.RuleVersion);
    }

    private EventStream(Guid id, TState state, Func<TState, TEvent, TState> reduce,
        ImmutableList<TEvent> events, ImmutableList<TEvent> pendingEvents,
        ImmutableHashSet<Guid> eventIds, long persistedVersion, long version, long historyStartVersion, string? ruleVersion = null)
    {
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(reduce);
        RuleVersion = ruleVersion;
        Id = id;
        State = state;
        _reduce = reduce;
        _events = events;
        _pendingEvents = pendingEvents;
        _eventIds = eventIds;
        PersistedVersion = persistedVersion;
        Version = version;
        HistoryStartVersion = historyStartVersion;
    }

    /// <summary>Return a new branch after cloning, validating and reducing one unrecorded event.</summary>
    public EventStream<TState, TEvent> Apply(TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (_eventIds.Contains(@event.Id)) throw new DuplicateEventException(@event.Id, Id);
        var tagged = EventHistory.AssignMetadata(@event, Id, checked(Version + 1));
        var state = _reduce(State, tagged) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(Id, state, _reduce, _events.Add(tagged), _pendingEvents.Add(tagged),
            _eventIds.Add(tagged.Id), PersistedVersion, checked(Version + 1), HistoryStartVersion, RuleVersion);
    }

    /// <summary>Capture committed state using the bound rule identity; unbound delegates must be migrated first.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(int schemaVersion) => CaptureSnapshot(
        RuleVersion ?? throw new InvalidOperationException("Bind a rule identity before automatic snapshot capture."), schemaVersion);

    /// <summary>Capture only committed state; an explicit rule label must match an already bound calculation.</summary>
    public AggregateSnapshot<TState> CaptureSnapshot(string ruleVersion, int schemaVersion)
    {
        if (RuleVersion is not null && !string.Equals(RuleVersion, ruleVersion, StringComparison.Ordinal))
            throw new ReplayRuleVersionException(RuleVersion, ruleVersion, Id);
        if (PendingEvents.Count != 0)
            throw new InvalidOperationException("Save pending events before capturing a committed snapshot.");
        return new(Id, PersistedVersion, State, ruleVersion, schemaVersion);
    }

    /// <summary>Restore with the expected state schema and the calculation's bound rules.</summary>
    public static EventStream<TState, TEvent> Replay(AggregateSnapshot<TState> snapshot,
        int schemaVersion, IEventReducer<TState, TEvent> reducer, StreamSlice<TEvent> suffix)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return Replay(snapshot, reducer.RuleVersion, schemaVersion, reducer.Reduce, suffix);
    }

    /// <summary>Load a checkpoint and validated suffix using the calculation's bound rule identity.</summary>
    public static Task<EventStream<TState, TEvent>> LoadAsync(Guid id, ReplayDefinition<TState> definition,
        int schemaVersion, IEventReducer<TState, TEvent> reducer, IEventStreamStore<TEvent> store,
        ISnapshotStore<TState> snapshots, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return LoadAsync(id, definition, reducer.RuleVersion, schemaVersion, reducer.Reduce, store, snapshots, cancellationToken);
    }

    /// <summary>Restore a snapshot with the identified calculation and its committed suffix.</summary>
    public static EventStream<TState, TEvent> Replay(AggregateSnapshot<TState> snapshot,
        IEventReducer<TState, TEvent> reducer, StreamSlice<TEvent> suffix)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        ArgumentNullException.ThrowIfNull(snapshot);
        return Replay(snapshot, reducer.RuleVersion, snapshot.SchemaVersion, reducer.Reduce, suffix);
    }

    /// <summary>Replay opening inputs with an identified calculation, rejecting mismatched rules.</summary>
    public static EventStream<TState, TEvent> Replay(Guid id, ReplayDefinition<TState> definition,
        IEventReducer<TState, TEvent> reducer, IEnumerable<TEvent> history)
    {
        ArgumentNullException.ThrowIfNull(reducer);
        return Replay(id, definition, reducer.RuleVersion, reducer.Reduce, history);
    }

    /// <summary>Load a compatible checkpoint and suffix, or replay from the opening definition if no checkpoint exists.</summary>
    public static async Task<EventStream<TState, TEvent>> LoadAsync(Guid id, ReplayDefinition<TState> definition,
        string reducerRuleVersion, int schemaVersion, Func<TState, TEvent, TState> reduce,
        IEventStreamStore<TEvent> store, ISnapshotStore<TState> snapshots,
        CancellationToken cancellationToken = default)
    {
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrWhiteSpace(reducerRuleVersion);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        ArgumentNullException.ThrowIfNull(reduce);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(snapshots);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(definition.RuleVersion, reducerRuleVersion, StringComparison.Ordinal))
            throw new ReplayRuleVersionException(definition.RuleVersion, reducerRuleVersion, id);
        var snapshot = await snapshots.ReadAsync(id, reducerRuleVersion, schemaVersion, cancellationToken);
        if (snapshot is not null && (snapshot.AggregateId != id ||
            snapshot.RuleVersion != reducerRuleVersion || snapshot.SchemaVersion != schemaVersion))
            throw new InvalidOperationException("Snapshot storage returned an incompatible checkpoint.");
        var cursor = snapshot?.StreamVersion ?? 0;
        cancellationToken.ThrowIfCancellationRequested();
        var suffix = await store.ReadFromAsync(id, cursor, cancellationToken);
        if (suffix.StreamId != id || suffix.AfterVersion != cursor)
            throw new InvalidOperationException("Event storage returned a suffix for a different stream or cursor.");
        cancellationToken.ThrowIfCancellationRequested();
        return snapshot is null
            ? Replay(id, definition, reducerRuleVersion, reduce, suffix.Events)
            : Replay(snapshot, reducerRuleVersion, schemaVersion, reduce, suffix);
    }

    /// <summary>Restore complete business state, then reduce only the validated committed suffix.</summary>
    public static EventStream<TState, TEvent> Replay(AggregateSnapshot<TState> snapshot,
        string reducerRuleVersion, int schemaVersion, Func<TState, TEvent, TState> reduce, StreamSlice<TEvent> suffix)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(reduce);
        ArgumentNullException.ThrowIfNull(suffix);
        ArgumentException.ThrowIfNullOrWhiteSpace(reducerRuleVersion);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        if (!string.Equals(snapshot.RuleVersion, reducerRuleVersion, StringComparison.Ordinal))
            throw new ReplayRuleVersionException(snapshot.RuleVersion, reducerRuleVersion, snapshot.AggregateId);
        if (snapshot.SchemaVersion != schemaVersion)
            throw new SnapshotSchemaVersionException(snapshot.SchemaVersion, schemaVersion, snapshot.AggregateId);
        if (snapshot.AggregateId != suffix.StreamId || snapshot.StreamVersion != suffix.AfterVersion)
            throw new InvalidOperationException("Snapshot identity and version must match the stream suffix.");
        var state = snapshot.State;
        foreach (var e in suffix.Events)
            state = reduce(state, e) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(snapshot.AggregateId, state, reduce, suffix.Events.ToImmutableList(),
            ImmutableList<TEvent>.Empty, suffix.EventIds, suffix.Version, suffix.Version, snapshot.StreamVersion, reducerRuleVersion);
    }

    /// <summary>
    /// Replay with application-owned persisted opening inputs and an explicitly identified reducer.
    /// A version mismatch is rejected before reducing history. The caller is responsible for
    /// identifying the reducer honestly and loading the definition belonging to this stream.
    /// </summary>
    public static EventStream<TState, TEvent> Replay(Guid id, ReplayDefinition<TState> definition,
        string reducerRuleVersion, Func<TState, TEvent, TState> reduce, IEnumerable<TEvent> history)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrWhiteSpace(reducerRuleVersion);
        if (!string.Equals(definition.RuleVersion, reducerRuleVersion, StringComparison.Ordinal))
            throw new ReplayRuleVersionException(definition.RuleVersion, reducerRuleVersion, id);
        return ReplayCore(id, definition.OpeningState, reduce, history, reducerRuleVersion);
    }

    /// <summary>
    /// Low-level replay: the caller must supply the original opening state and matching rules.
    /// This overload does not verify rule provenance; use ReplayDefinition for persisted scenarios.
    /// </summary>
    public static EventStream<TState, TEvent> Replay(Guid id, TState openingState,
        Func<TState, TEvent, TState> reduce, IEnumerable<TEvent> history)
        => ReplayCore(id, openingState, reduce, history, null);

    private static EventStream<TState, TEvent> ReplayCore(Guid id, TState openingState,
        Func<TState, TEvent, TState> reduce, IEnumerable<TEvent> history, string? ruleVersion)
    {
        ArgumentNullException.ThrowIfNull(history);
        ArgumentNullException.ThrowIfNull(openingState);
        ArgumentNullException.ThrowIfNull(reduce);
        var events = history.ToImmutableList();
        EventHistory.Validate(id, events);
        var state = openingState;
        foreach (var @event in events)
            state = reduce(state, @event) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(id, state, reduce, events, ImmutableList<TEvent>.Empty,
            events.Select(e => e.Id).ToImmutableHashSet(), events.Count, events.Count, 0, ruleVersion);
    }

    /// <summary>Commit pending events atomically; return a saved view without changing the original on success or failure.</summary>
    public async Task<EventStream<TState, TEvent>> SaveAsync(IEventStreamStore<TEvent> store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        if (PendingEvents.Count == 0) return this;
        var saved = AsSaved();
        await store.AppendAsync(Id, PersistedVersion, PendingEvents, cancellationToken);
        return saved;
    }

    internal EventStream<TState, TEvent> AsSaved() => PendingEvents.Count == 0
        ? this : new(Id, State, _reduce, _events, ImmutableList<TEvent>.Empty, _eventIds,
            Version, Version, HistoryStartVersion, RuleVersion);

    /// <summary>Apply one event and atomically save the resulting pending history.</summary>
    public Task<EventStream<TState, TEvent>> HandleAsync(TEvent @event, IEventStreamStore<TEvent> store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return Apply(@event).SaveAsync(store, cancellationToken);
    }
}
