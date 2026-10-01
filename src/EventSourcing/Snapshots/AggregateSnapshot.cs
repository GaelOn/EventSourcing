namespace EventSourcing.Snapshots;

/// <summary>
/// A rebuildable checkpoint of complete immutable business state at an exact persisted stream version.
/// The application owns the state shape and must retain the matching rules and opening definition.
/// </summary>
public sealed record AggregateSnapshot<TState> where TState : class
{
    /// <summary>Identity of the aggregate represented by this checkpoint.</summary>
    public Guid AggregateId { get; }
    /// <summary>Committed position represented by this checkpoint.</summary>
    public long StreamVersion { get; }
    /// <summary>Complete immutable business state at the current version.</summary>
    public TState State { get; }
    /// <summary>Application-owned identity of the calculation rules and immutable configuration.</summary>
    public string RuleVersion { get; }
    /// <summary>Positive version of the application-owned state serialization schema.</summary>
    public int SchemaVersion { get; }

    /// <summary>Create a validated checkpoint of complete committed state.</summary>
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

/// <summary>A checkpoint uses an incompatible business state schema.</summary>
public sealed class SnapshotSchemaVersionException : InvalidOperationException
{
    /// <summary>The checkpoint schema.</summary>
    public int RecordedVersion { get; }
    /// <summary>The schema expected by the consumer.</summary>
    public int ExpectedVersion { get; }
    /// <summary>The affected aggregate when known.</summary>
    public Guid? AggregateId { get; }
    /// <summary>Create a schema compatibility diagnostic.</summary>
    public SnapshotSchemaVersionException(int recordedVersion, int expectedVersion, Guid? aggregateId = null)
        : base($"Snapshot schema is {recordedVersion}, but restoration requires schema {expectedVersion}.")
    { RecordedVersion = recordedVersion; ExpectedVersion = expectedVersion; AggregateId = aggregateId; }
}
