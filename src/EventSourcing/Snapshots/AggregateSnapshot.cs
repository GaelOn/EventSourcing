namespace EventSourcing.Snapshots;

/// <summary>
/// A rebuildable checkpoint of complete immutable business state at an exact persisted stream version.
/// The application owns the state shape and must retain the matching rules and opening definition.
/// </summary>
public sealed record AggregateSnapshot<TState> where TState : class
{
    public Guid AggregateId { get; }
    public long StreamVersion { get; }
    public TState State { get; }
    public string RuleVersion { get; }
    public int SchemaVersion { get; }

    public AggregateSnapshot(Guid aggregateId, long streamVersion, TState state,
        string ruleVersion, int schemaVersion)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("An aggregate ID is required.", nameof(aggregateId));
        if (streamVersion < 0) throw new ArgumentOutOfRangeException(nameof(streamVersion));
        ArgumentNullException.ThrowIfNull(state);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        AggregateId = aggregateId;
        StreamVersion = streamVersion;
        State = state;
        RuleVersion = ruleVersion;
        SchemaVersion = schemaVersion;
    }
}

public sealed class SnapshotSchemaVersionException(int recordedVersion, int expectedVersion)
    : InvalidOperationException($"Snapshot schema is {recordedVersion}, but restoration requires schema {expectedVersion}.");
