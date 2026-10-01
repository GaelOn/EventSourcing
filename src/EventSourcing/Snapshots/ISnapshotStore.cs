namespace EventSourcing.Snapshots;

/// <summary>Optional checkpoint cache. Event history remains authoritative and is never deleted here.</summary>
public interface ISnapshotStore<TState> where TState : class
{
    /// <summary>
    /// Return the newest checkpoint matching aggregate ID, rule version, and schema version, or null.
    /// Cache absence or incompatible formats permit full replay; storage/corruption errors propagate.
    /// </summary>
    Task<AggregateSnapshot<TState>?> ReadAsync(Guid aggregateId, string ruleVersion, int schemaVersion,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically save a checkpoint. Ignore older/equal versions for the same ID/rules/schema.
    /// Callers must capture committed state; this cache cannot verify arbitrary business state against events.
    /// Failure leaves previous checkpoints unchanged and does not undo an earlier event commit.
    /// Cancellation before commit preserves the cache; once committed the operation reports success.
    /// </summary>
    Task SaveAsync(AggregateSnapshot<TState> snapshot, CancellationToken cancellationToken = default);
}
