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
    public Guid Id { get; }
    public TState State { get; }
    public IReadOnlyList<TEvent> Events => _events;
    public IReadOnlyList<TEvent> PendingEvents => _pendingEvents;
    public long Version { get; }
    public long PersistedVersion { get; }
    /// <summary>Events contains only the history after this snapshot cursor; zero means opening state.</summary>
    public long HistoryStartVersion { get; }

    public EventStream(Guid id, TState openingState, Func<TState, TEvent, TState> reduce)
        : this(id, openingState, reduce, ImmutableList<TEvent>.Empty,
            ImmutableList<TEvent>.Empty, ImmutableHashSet<Guid>.Empty, 0, 0, 0) { }

    private EventStream(Guid id, TState state, Func<TState, TEvent, TState> reduce,
        ImmutableList<TEvent> events, ImmutableList<TEvent> pendingEvents,
        ImmutableHashSet<Guid> eventIds, long persistedVersion, long version, long historyStartVersion)
    {
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(reduce);
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

    public EventStream<TState, TEvent> Apply(TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (_eventIds.Contains(@event.Id)) throw new DuplicateEventException(@event.Id);
        var tagged = EventHistory.AssignMetadata(@event, Id, checked(Version + 1));
        var state = _reduce(State, tagged) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(Id, state, _reduce, _events.Add(tagged), _pendingEvents.Add(tagged),
            _eventIds.Add(tagged.Id), PersistedVersion, checked(Version + 1), HistoryStartVersion);
    }

    public AggregateSnapshot<TState> CaptureSnapshot(string ruleVersion, int schemaVersion)
    {
        if (PendingEvents.Count != 0)
            throw new InvalidOperationException("Save pending events before capturing a committed snapshot.");
        return new(Id, PersistedVersion, State, ruleVersion, schemaVersion);
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
            throw new ReplayRuleVersionException(definition.RuleVersion, reducerRuleVersion);
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
            throw new ReplayRuleVersionException(snapshot.RuleVersion, reducerRuleVersion);
        if (snapshot.SchemaVersion != schemaVersion)
            throw new SnapshotSchemaVersionException(snapshot.SchemaVersion, schemaVersion);
        if (snapshot.AggregateId != suffix.StreamId || snapshot.StreamVersion != suffix.AfterVersion)
            throw new InvalidOperationException("Snapshot identity and version must match the stream suffix.");
        var state = snapshot.State;
        foreach (var e in suffix.Events)
            state = reduce(state, e) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(snapshot.AggregateId, state, reduce, suffix.Events.ToImmutableList(),
            ImmutableList<TEvent>.Empty, suffix.EventIds, suffix.Version, suffix.Version, snapshot.StreamVersion);
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
            throw new ReplayRuleVersionException(definition.RuleVersion, reducerRuleVersion);
        return Replay(id, definition.OpeningState, reduce, history);
    }

    /// <summary>
    /// Low-level replay: the caller must supply the original opening state and matching rules.
    /// This overload does not verify rule provenance; use ReplayDefinition for persisted scenarios.
    /// </summary>
    public static EventStream<TState, TEvent> Replay(Guid id, TState openingState,
        Func<TState, TEvent, TState> reduce, IEnumerable<TEvent> history)
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
            events.Select(e => e.Id).ToImmutableHashSet(), events.Count, events.Count, 0);
    }

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
            Version, Version, HistoryStartVersion);

    public Task<EventStream<TState, TEvent>> HandleAsync(TEvent @event, IEventStreamStore<TEvent> store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return Apply(@event).SaveAsync(store, cancellationToken);
    }
}
