using EventSourcing.Streams;

namespace EventSourcing.ObjectsAggregation;

/// <summary>Atomic publication of roots with different state types and unrelated event sets.</summary>
public static class EventSourcedAggregateBatch
{
    /// <summary>Prepare consistent saved roots before I/O and assert every participant's expected version.</summary>
    public static async Task<IReadOnlyList<IEventSourcedAggregate>> SaveAsync(IAtomicEventStreamStore store,
        IEnumerable<IEventSourcedAggregate> aggregates, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(aggregates);
        cancellationToken.ThrowIfCancellationRequested();
        var participants = aggregates.ToArray();
        var ids = new HashSet<Guid>();
        foreach (var root in participants)
        {
            ArgumentNullException.ThrowIfNull(root);
            if (!ids.Add(root.Id)) throw new ArgumentException("Each aggregate can participate only once.", nameof(aggregates));
            if (root is not IPreparedAggregate) throw new ArgumentException("Use framework aggregate roots for safe commit publication.", nameof(aggregates));
        }
        var saved = Array.AsReadOnly(participants.Select(a => ((IPreparedAggregate)a).PrepareSaved()).ToArray());
        var appends = participants.Select(a => new StreamAppend(a.Id, a.PersistedVersion, a.PendingEvents)).ToArray();
        if (appends.Length > 0) await store.AppendBatchAsync(appends, cancellationToken).ConfigureAwait(false);
        return saved;
    }

    /// <summary>Typed convenience for participants sharing a state type; preserves each runtime business-root type.</summary>
    public static async Task<IReadOnlyList<EventSourcedAggregate<TState>>> SaveAsync<TState>(IAtomicEventStreamStore store,
        IEnumerable<EventSourcedAggregate<TState>> aggregates, CancellationToken cancellationToken = default) where TState : class
    {
        ArgumentNullException.ThrowIfNull(aggregates);
        var saved = await SaveAsync(store, aggregates.Cast<IEventSourcedAggregate>(), cancellationToken).ConfigureAwait(false);
        return Array.AsReadOnly(saved.Cast<EventSourcedAggregate<TState>>().ToArray());
    }
}
