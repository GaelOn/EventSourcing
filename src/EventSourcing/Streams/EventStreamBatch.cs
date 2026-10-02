namespace EventSourcing.Streams;

/// <summary>State-free atomic persistence, including unchanged participants as read dependencies.</summary>
public static class EventStreamBatch
{
    /// <summary>Prepare all saved views, then commit every expected version and append in one transaction.</summary>
    public static async Task<IReadOnlyList<EventStream>> SaveAsync(IAtomicEventStreamStore store,
        IEnumerable<EventStream> streams, CancellationToken cancellationToken = default)
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
        var saved = Array.AsReadOnly(participants.Select(s => s.AsSaved()).ToArray());
        var appends = Array.AsReadOnly(participants.Select(s => new StreamAppend(s.Id, s.PersistedVersion, s.PendingEvents)).ToArray());
        if (appends.Count > 0) await store.AppendBatchAsync(appends, cancellationToken).ConfigureAwait(false);
        return saved;
    }
}
