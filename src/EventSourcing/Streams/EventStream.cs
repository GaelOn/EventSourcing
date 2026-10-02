using System.Collections.Immutable;

namespace EventSourcing.Streams;

/// <summary>
/// Immutable heterogeneous history and persistence lifecycle. This view contains no business state
/// or transition rules. Payloads must be deeply immutable; append does not apply business rules.
/// </summary>
public sealed class EventStream
{
    private readonly ImmutableList<IEvent> _events;
    private readonly ImmutableList<IEvent> _pendingEvents;
    private readonly ImmutableHashSet<Guid> _eventIds;
    /// <summary>Caller-supplied aggregate identity.</summary>
    public Guid Id { get; }
    /// <summary>Accepted position, including pending events.</summary>
    public long Version { get; }
    /// <summary>Last successfully committed position.</summary>
    public long PersistedVersion { get; }
    /// <summary>Checkpoint cursor preceding the retained suffix.</summary>
    public long HistoryStartVersion { get; }
    /// <summary>Ordered accepted events after the checkpoint boundary.</summary>
    public IReadOnlyList<IEvent> Events => _events;
    /// <summary>Events awaiting atomic persistence.</summary>
    public IReadOnlyList<IEvent> PendingEvents => _pendingEvents;
    /// <summary>Whether this view has uncommitted events.</summary>
    public bool HasPendingChanges => _pendingEvents.Count != 0;

    /// <summary>Create an empty stream without state or reducers.</summary>
    public EventStream(Guid id) : this(id, ImmutableList<IEvent>.Empty, ImmutableList<IEvent>.Empty,
        ImmutableHashSet<Guid>.Empty, 0, 0, 0) { }

    private EventStream(Guid id, ImmutableList<IEvent> events, ImmutableList<IEvent> pending,
        ImmutableHashSet<Guid> ids, long version, long persistedVersion, long cursor)
    {
        if (id == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(id));
        Id = id; _events = events; _pendingEvents = pending; _eventIds = ids;
        Version = version; PersistedVersion = persistedVersion; HistoryStartVersion = cursor;
    }

    /// <summary>Validate and tag a clone, then return a new history branch without reducing business state.</summary>
    public EventStream Append(IEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (_eventIds.Contains(@event.Id)) throw new DuplicateEventException(@event.Id, Id);
        var version = checked(Version + 1);
        var tagged = EventHistory.AssignMetadata(@event, Id, version);
        return new(Id, _events.Add(tagged), _pendingEvents.Add(tagged), _eventIds.Add(tagged.Id),
            version, PersistedVersion, HistoryStartVersion);
    }

    /// <summary>Restore validated committed full history without business reduction.</summary>
    public static EventStream Restore(Guid id, IEnumerable<IEvent> history)
    {
        ArgumentNullException.ThrowIfNull(history);
        var events = history.ToImmutableList();
        EventHistory.Validate(id, events);
        return new(id, events, ImmutableList<IEvent>.Empty, events.Select(e => e.Id).ToImmutableHashSet(),
            events.Count, events.Count, 0);
    }

    /// <summary>Restore a validated suffix and its complete prefix identity index; the caller owns checkpoint state.</summary>
    public static EventStream Restore(StreamSlice slice)
    {
        ArgumentNullException.ThrowIfNull(slice);
        return new(slice.StreamId, slice.Events.ToImmutableList(), ImmutableList<IEvent>.Empty,
            slice.EventIds, slice.Version, slice.Version, slice.AfterVersion);
    }

    /// <summary>Persist the whole pending batch; prepare the returned view before I/O.</summary>
    public async Task<EventStream> SaveAsync(IEventStreamStore store, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        cancellationToken.ThrowIfCancellationRequested();
        if (!HasPendingChanges) return this;
        var saved = AsSaved();
        await store.AppendAsync(Id, PersistedVersion, PendingEvents, cancellationToken).ConfigureAwait(false);
        return saved;
    }

    internal EventStream AsSaved() => !HasPendingChanges ? this : new(Id, _events,
        ImmutableList<IEvent>.Empty, _eventIds, Version, Version, HistoryStartVersion);
}
