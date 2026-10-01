namespace EventSourcing.Streams;

/// <summary>Save related immutable streams through one atomic storage operation.</summary>
public static class EventStreamBatch
{
    /// <summary>Commit pending events atomically; return a saved view without changing the original on success or failure.</summary>
    public static async Task<IReadOnlyList<EventStream<TState, TEvent>>> SaveAsync<TState, TEvent>(
        IAtomicEventStreamStore<TEvent> store, IEnumerable<EventStream<TState, TEvent>> streams,
        CancellationToken cancellationToken = default)
        where TState : class
        where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(streams);
        cancellationToken.ThrowIfCancellationRequested();
        var participants = streams.ToArray();
        var ids = new HashSet<Guid>();
        foreach (var stream in participants)
        {
            ArgumentNullException.ThrowIfNull(stream);
            if (!ids.Add(stream.Id)) throw new ArgumentException("Each stream can participate only once.", nameof(streams));
        }
        // Build return values before persistence so no user code executes after the commit.
        var saved = Array.AsReadOnly(participants.Select(stream => stream.AsSaved()).ToArray());
        var appends = Array.AsReadOnly(participants.Select(stream =>
            new StreamAppend<TEvent>(stream.Id, stream.PersistedVersion, stream.PendingEvents)).ToArray());
        if (appends.Count > 0)
            await store.AppendBatchAsync(appends, cancellationToken);
        return saved;
    }
}
