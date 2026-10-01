namespace EventSourcing.Streams;

/// <summary>
/// Preferred aggregate API: immutable stream with a pure reducer shared by live calculation and replay.
/// Instances may be shared between threads when events/state are immutable and the reducer is pure.
/// State and events must be immutable; the reducer must not mutate inputs or perform I/O.
/// </summary>
public sealed class EventStream<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    private readonly Func<TState, TEvent, TState> _reduce;
    public Guid Id { get; }
    public TState State { get; }
    public IReadOnlyList<TEvent> Events { get; }
    public IReadOnlyList<TEvent> PendingEvents { get; }
    public long Version => Events.Count;
    public long PersistedVersion { get; }

    public EventStream(Guid id, TState openingState, Func<TState, TEvent, TState> reduce)
        : this(id, openingState, reduce, [], 0) { }

    private EventStream(Guid id, TState state, Func<TState, TEvent, TState> reduce,
        TEvent[] events, long persistedVersion)
    {
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(reduce);
        Id = id;
        State = state;
        _reduce = reduce;
        Events = Array.AsReadOnly(events);
        PersistedVersion = persistedVersion;
        PendingEvents = Array.AsReadOnly(events.Skip(checked((int)persistedVersion)).ToArray());
    }

    public EventStream<TState, TEvent> Apply(TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (Events.Any(existing => existing.Id == @event.Id)) throw new DuplicateEventException(@event.Id);
        var tagged = EventHistory.Tag(@event, Id, checked(Version + 1));
        var state = _reduce(State, tagged) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(Id, state, _reduce, [.. Events, tagged], PersistedVersion);
    }

    public static EventStream<TState, TEvent> Replay(Guid id, TState openingState,
        Func<TState, TEvent, TState> reduce, IEnumerable<TEvent> history)
    {
        ArgumentNullException.ThrowIfNull(history);
        ArgumentNullException.ThrowIfNull(openingState);
        ArgumentNullException.ThrowIfNull(reduce);
        var events = history.ToArray();
        EventHistory.Validate(id, events);
        var state = openingState;
        foreach (var @event in events)
            state = reduce(state, @event) ?? throw new InvalidOperationException("Reducer returned null state.");
        return new(id, state, reduce, events, events.Length);
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
        ? this : new(Id, State, _reduce, Events.ToArray(), Version);

    public Task<EventStream<TState, TEvent>> HandleAsync(TEvent @event, IEventStreamStore<TEvent> store,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        return Apply(@event).SaveAsync(store, cancellationToken);
    }
}
