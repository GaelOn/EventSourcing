using EventSourcing.Streams;

namespace EventSourcing.Transactions;

/// <summary>Opaque durable semantic identity. Namespace must include the client's key algorithm version.</summary>
public sealed record OperationKey
{
    /// <summary>Explicit application/tenant/key-version scope; ordinal and case sensitive.</summary>
    public string Namespace { get; }
    /// <summary>Stable client-supplied identity of one intended operation.</summary>
    public string Value { get; }
    /// <summary>No trimming, hashing, normalization or payload comparison is inferred.</summary>
    public OperationKey(string @namespace, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(@namespace);
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (@namespace.Length > 512 || value.Length > 2048)
            throw new ArgumentException("Operation identity exceeds its supported length.");
        Namespace = @namespace; Value = value;
    }
}

/// <summary>Admission distinguishes expected duplicates from unresolved owners and replay suppression.</summary>
public enum OperationStatus { Claimed, Committed, AlreadyConsumed, InProgress, RetryAllowed, ReplaySuppressed }

/// <summary>Immutable result; consumed outcomes are retained even for decisions with no domain events.</summary>
public sealed record OperationResult(OperationStatus Status, TransactionHandle? Handle = null,
    OperationJournalEvent? Receipt = null);

/// <summary>Exclusive process/session identity of a store. Reopening durable storage obtains a new session.</summary>
public interface ITransactionSessionStore : IAtomicEventStreamStore
{
    /// <summary>Old sessions cannot be active while recovery is permitted by this store.</summary>
    Guid SessionId { get; }
}

/// <summary>Isolated consequences and durable opaque outcome. Participants include unchanged read dependencies.</summary>
public sealed class OperationPlan
{
    /// <summary>Whole atomic batch. Payloads must be deeply immutable.</summary>
    public IReadOnlyList<StreamAppend> Appends { get; }
    /// <summary>Optional client-defined versioned result; never used as semantic identity.</summary>
    public string? Outcome { get; }
    /// <summary>Copy the request collection before publication.</summary>
    public OperationPlan(IEnumerable<StreamAppend> appends, string? outcome = null)
    {
        ArgumentNullException.ThrowIfNull(appends);
        Appends = Array.AsReadOnly(appends.ToArray()); Outcome = outcome;
        if (Appends.Any(a => a is null) || Appends.Select(a => a.StreamId).Distinct().Count() != Appends.Count)
            throw new ArgumentException("Each non-null stream must participate only once.", nameof(appends));
    }
}

/// <summary>Opaque ownership capability. Only the claiming coordinator can stage or finish this attempt.</summary>
public sealed class TransactionHandle
{
    internal object Coordinator { get; }
    internal OperationPlan? Plan { get; set; }
    /// <summary>Stable operation identity.</summary>
    public OperationKey Key { get; }
    /// <summary>Immutable processing contract identity.</summary>
    public string ContractVersion { get; }
    /// <summary>Attempt identity, distinct from operation and domain event identities.</summary>
    public Guid AttemptId { get; }
    internal TransactionHandle(object coordinator, OperationKey key, string contractVersion, Guid attemptId)
    { Coordinator = coordinator; Key = key; ContractVersion = contractVersion; AttemptId = attemptId; }
}

/// <summary>Injectable local transaction guarantees; applications do not depend on journal internals.</summary>
public interface IEventTransactionCoordinator
{
    /// <summary>Atomically claim or resolve an existing identity. Never runs consequences for duplicates.</summary>
    Task<OperationResult> BeginAsync(OperationKey key, string contractVersion, CancellationToken cancellationToken = default);
    /// <summary>Stage already isolated immutable consequences. Does not publish business state.</summary>
    Task StageAsync(TransactionHandle handle, OperationPlan plan, CancellationToken cancellationToken = default);
    /// <summary>Revalidate ownership and every participant; publish complete batch and receipt atomically.</summary>
    Task<OperationResult> CommitAsync(TransactionHandle handle, CancellationToken cancellationToken = default);
    /// <summary>Complete rollback before retry. A consumed transaction is never rolled back.</summary>
    Task RollbackAsync(TransactionHandle handle, CancellationToken cancellationToken = default);
    /// <summary>Bounded cancellable duplicate resolution; timeout never authorizes another executor.</summary>
    Task<OperationResult> ResolveAsync(OperationKey key, string contractVersion, TimeSpan wait,
        CancellationToken cancellationToken = default);
    /// <summary>Recover abandoned older-session claims under the store's exclusive ownership guarantee.</summary>
    Task<int> RecoverAsync(CancellationToken cancellationToken = default);
}

/// <summary>Corrupt evidence, changed identity contracts and stale owners fail explicitly.</summary>
public sealed class OperationIntegrityException(string message) : InvalidOperationException(message);
