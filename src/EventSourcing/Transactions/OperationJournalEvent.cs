using System.Text.Json.Serialization;

namespace EventSourcing.Transactions;

/// <summary>Durable lifecycle markers; only Committed is an authoritative operation outcome.</summary>
public enum OperationJournalKind { Claimed, Committed, RolledBack }

/// <summary>Exact committed participant range, including unchanged expected-version dependencies.</summary>
public sealed class OperationRange
{
    /// <summary>Participant identity.</summary>
    public Guid StreamId { get; }
    /// <summary>Position before the operation.</summary>
    public long BeforeVersion { get; }
    /// <summary>Exact ordered newly committed event identities.</summary>
    public IReadOnlyList<Guid> EventIds { get; }
    /// <summary>Validate and freeze receipt evidence.</summary>
    [JsonConstructor]
    public OperationRange(Guid streamId, long beforeVersion, IReadOnlyList<Guid> eventIds)
    {
        ArgumentNullException.ThrowIfNull(eventIds);
        if (streamId == Guid.Empty || beforeVersion < 0 || eventIds.Any(id => id == Guid.Empty)
            || eventIds.Distinct().Count() != eventIds.Count)
            throw new OperationIntegrityException("Invalid committed operation range.");
        StreamId = streamId; BeforeVersion = beforeVersion; EventIds = Array.AsReadOnly(eventIds.ToArray());
    }
}

/// <summary>Version-one journal wire contract. It is separate from domain events and owns no business state.</summary>
public sealed class OperationJournalEvent : EventBase
{
    /// <summary>Semantic identity.</summary>
    public OperationKey Key { get; }
    /// <summary>Immutable client processing contract.</summary>
    public string ContractVersion { get; }
    /// <summary>Owner attempt.</summary>
    public Guid AttemptId { get; }
    /// <summary>Exclusive storage session.</summary>
    public Guid OwnerSessionId { get; }
    /// <summary>Lifecycle transition.</summary>
    public OperationJournalKind Kind { get; }
    /// <summary>Committed ordered participant evidence.</summary>
    public IReadOnlyList<OperationRange> Ranges { get; }
    /// <summary>Client's retained result, including no-op decisions.</summary>
    public string? Outcome { get; }
    /// <summary>Validate journal data at construction and deserialization.</summary>
    [JsonConstructor]
    public OperationJournalEvent(Guid id, EventMetadata metadata, OperationKey key, string contractVersion,
        Guid attemptId, Guid ownerSessionId, OperationJournalKind kind, IReadOnlyList<OperationRange> ranges,
        string? outcome = null) : base(id, metadata)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(contractVersion);
        ArgumentNullException.ThrowIfNull(ranges);
        if (attemptId == Guid.Empty || ownerSessionId == Guid.Empty || !Enum.IsDefined(kind)
            || ranges.Any(r => r is null) || ranges.Select(r => r.StreamId).Distinct().Count() != ranges.Count
            || (kind != OperationJournalKind.Committed && (ranges.Count != 0 || outcome is not null)))
            throw new OperationIntegrityException("Invalid transaction journal evidence.");
        Key = key; ContractVersion = contractVersion; AttemptId = attemptId; OwnerSessionId = ownerSessionId;
        Kind = kind; Ranges = Array.AsReadOnly(ranges.ToArray()); Outcome = outcome;
    }
    /// <inheritdoc />
    public override IEvent Clone(EventMetadata metadata) => new OperationJournalEvent(Id, metadata, Key,
        ContractVersion, AttemptId, OwnerSessionId, Kind, Ranges, Outcome);
}
