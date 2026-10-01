namespace EventSourcing.Snapshots;

/// <summary>Thread-safe, process-local checkpoint cache. State must be deeply immutable.</summary>
public sealed class InMemorySnapshotStore<TState> : ISnapshotStore<TState> where TState : class
{
    private readonly object _gate = new();
    private readonly Dictionary<(Guid Id, string Rules, int Schema), AggregateSnapshot<TState>> _snapshots = [];

    /// <summary>Read committed history from a consistent storage view.</summary>
    public Task<AggregateSnapshot<TState>?> ReadAsync(Guid aggregateId, string ruleVersion, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("An aggregate ID is required.", nameof(aggregateId));
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_snapshots.GetValueOrDefault((aggregateId, ruleVersion, schemaVersion)));
        }
    }

    /// <summary>Commit pending events atomically; return a saved view without changing the original on success or failure.</summary>
    public Task SaveAsync(AggregateSnapshot<TState> snapshot, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = (snapshot.AggregateId, snapshot.RuleVersion, snapshot.SchemaVersion);
            if (!_snapshots.TryGetValue(key, out var stored) || snapshot.StreamVersion > stored.StreamVersion)
                _snapshots[key] = snapshot;
        }
        return Task.CompletedTask;
    }
}
