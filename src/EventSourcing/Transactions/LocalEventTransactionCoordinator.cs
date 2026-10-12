using System.Runtime.CompilerServices;
using EventSourcing.Streams;

namespace EventSourcing.Transactions;

/// <summary>Single-store coordination. Claims and receipts use one reserved stream within the atomic boundary.</summary>
public sealed class LocalEventTransactionCoordinator : IEventTransactionCoordinator
{
    private static readonly ConditionalWeakTable<ITransactionSessionStore, SemaphoreSlim> Gates = new();
    private readonly ITransactionSessionStore _store;
    private readonly SemaphoreSlim _gate;
    /// <summary>Explicit reserved journal stream; callers must not append business events to it.</summary>
    public Guid JournalStreamId { get; }
    /// <summary>Share the same store instance and journal identity among coordinators for one ledger.</summary>
    public LocalEventTransactionCoordinator(ITransactionSessionStore store, Guid journalStreamId)
    {
        ArgumentNullException.ThrowIfNull(store);
        if (journalStreamId == Guid.Empty || store.SessionId == Guid.Empty) throw new ArgumentException("Storage identities are required.");
        _store = store; JournalStreamId = journalStreamId; _gate = Gates.GetValue(store, _ => new(1, 1));
    }

    /// <inheritdoc />
    public async Task<OperationResult> BeginAsync(OperationKey key, string contractVersion, CancellationToken cancellationToken = default)
    {
        Validate(key, contractVersion);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var (journal, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var current = Find(states, key, contractVersion);
            if (current?.Kind == OperationJournalKind.Committed) return new(OperationStatus.AlreadyConsumed, Receipt: OperationReceipt.FromJournal(current));
            if (current?.Kind == OperationJournalKind.Claimed) return new(OperationStatus.InProgress);
            var handle = new TransactionHandle(this, key, contractVersion, Guid.NewGuid());
            try { await AppendMarkerAsync(journal, Marker(handle, OperationJournalKind.Claimed), cancellationToken).ConfigureAwait(false); }
            catch
            {
                var (_, confirmed) = await ReadAsync(CancellationToken.None).ConfigureAwait(false);
                var claimed = Find(confirmed, key, contractVersion);
                if (claimed?.Kind != OperationJournalKind.Claimed || claimed.AttemptId != handle.AttemptId
                    || claimed.OwnerSessionId != _store.SessionId) throw;
            }
            return new(OperationStatus.Claimed, handle);
        }
        finally { _gate.Release(); }
    }

    /// <inheritdoc />
    public async Task StageAsync(TransactionHandle handle, OperationPlan plan, CancellationToken cancellationToken = default)
    {
        ValidateHandle(handle); ArgumentNullException.ThrowIfNull(plan);
        if (plan.Appends.Any(a => a.StreamId == JournalStreamId || a.Events.Any(e => e is OperationJournalEvent)))
            throw new OperationIntegrityException("Business plans cannot write reserved transaction evidence.");
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var (_, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            RequireOwner(handle, Find(states, handle.Key, handle.ContractVersion));
            if (handle.Plan is not null) throw new OperationIntegrityException("Attempt is already staged.");
            handle.Plan = plan;
        }
        finally { _gate.Release(); }
    }

    /// <inheritdoc />
    public async Task<OperationResult> CommitAsync(TransactionHandle handle, CancellationToken cancellationToken = default)
    {
        ValidateHandle(handle);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var (journal, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var current = Find(states, handle.Key, handle.ContractVersion);
            if (current?.Kind == OperationJournalKind.Committed && current.AttemptId == handle.AttemptId)
                return new(OperationStatus.AlreadyConsumed, Receipt: OperationReceipt.FromJournal(current));
            RequireOwner(handle, current);
            var plan = handle.Plan ?? throw new OperationIntegrityException("Attempt has no staged plan.");
            var receipt = Marker(handle, OperationJournalKind.Committed, plan);
            var tagged = (OperationJournalEvent)EventHistory.AssignMetadata(receipt, JournalStreamId, journal.Count + 1L);
            var batch = plan.Appends.Concat(new[] { new StreamAppend(JournalStreamId, journal.Count, new[] { tagged }) }).ToArray();
            try { await _store.AppendBatchAsync(batch, cancellationToken).ConfigureAwait(false); }
            catch
            {
                // Storage may have published before losing its acknowledgement. Never undo a committed batch.
                var (_, confirmed) = await ReadAsync(CancellationToken.None).ConfigureAwait(false);
                var recorded = Find(confirmed, handle.Key, handle.ContractVersion);
                if (recorded?.Kind == OperationJournalKind.Committed && recorded.AttemptId == handle.AttemptId)
                    return new(OperationStatus.Committed, Receipt: OperationReceipt.FromJournal(recorded));
                throw;
            }
            return new(OperationStatus.Committed, Receipt: OperationReceipt.FromJournal(tagged));
        }
        finally { _gate.Release(); }
    }

    /// <inheritdoc />
    public async Task RollbackAsync(TransactionHandle handle, CancellationToken cancellationToken = default)
    {
        ValidateHandle(handle);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var (journal, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var current = Find(states, handle.Key, handle.ContractVersion);
            if (current?.AttemptId == handle.AttemptId && current.Kind is OperationJournalKind.Committed or OperationJournalKind.RolledBack) return;
            RequireOwner(handle, current);
            try { await AppendMarkerAsync(journal, Marker(handle, OperationJournalKind.RolledBack), cancellationToken).ConfigureAwait(false); }
            catch
            {
                var (_, confirmed) = await ReadAsync(CancellationToken.None).ConfigureAwait(false);
                var rolledBack = Find(confirmed, handle.Key, handle.ContractVersion);
                if (rolledBack?.Kind != OperationJournalKind.RolledBack || rolledBack.AttemptId != handle.AttemptId) throw;
            }
            handle.Plan = null;
        }
        finally { _gate.Release(); }
    }

    /// <inheritdoc />
    public async Task<OperationResult> ResolveAsync(OperationKey key, string contractVersion, TimeSpan wait,
        CancellationToken cancellationToken = default)
    {
        Validate(key, contractVersion);
        if (wait < TimeSpan.Zero || wait > TimeSpan.FromMinutes(5)) throw new ArgumentOutOfRangeException(nameof(wait));
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            var (_, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var current = Find(states, key, contractVersion);
            if (current?.Kind == OperationJournalKind.Committed) return new(OperationStatus.AlreadyConsumed, Receipt: OperationReceipt.FromJournal(current));
            if (current is null || current.Kind == OperationJournalKind.RolledBack) return new(OperationStatus.RetryAllowed);
            var remaining = wait - clock.Elapsed;
            if (remaining <= TimeSpan.Zero) return new(OperationStatus.InProgress);
            await Task.Delay(remaining < TimeSpan.FromMilliseconds(20) ? remaining : TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> RecoverAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var (journal, states) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var abandoned = states.Values.Where(e => e.Kind == OperationJournalKind.Claimed && e.OwnerSessionId != _store.SessionId).ToArray();
            var markers = abandoned.Select((e, index) => EventHistory.AssignMetadata(new OperationJournalEvent(
                Guid.NewGuid(), new(null, null), e.Key, e.ContractVersion, e.AttemptId, e.OwnerSessionId,
                OperationJournalKind.RolledBack, Array.Empty<OperationRange>()), JournalStreamId, journal.Count + index + 1L)).ToArray();
            if (markers.Length > 0) await _store.AppendAsync(JournalStreamId, journal.Count, markers, cancellationToken).ConfigureAwait(false);
            return markers.Length;
        }
        finally { _gate.Release(); }
    }

    private OperationJournalEvent Marker(TransactionHandle handle, OperationJournalKind kind, OperationPlan? plan = null) =>
        new(Guid.NewGuid(), new(null, null), handle.Key, handle.ContractVersion, handle.AttemptId, _store.SessionId,
            kind, plan?.Appends.Select(a => new OperationRange(a.StreamId, a.ExpectedVersion, a.Events.Select(e => e.Id).ToArray())).ToArray()
                ?? Array.Empty<OperationRange>(), plan?.Outcome);
    private Task AppendMarkerAsync(IReadOnlyList<IEvent> journal, OperationJournalEvent marker, CancellationToken token) =>
        _store.AppendAsync(JournalStreamId, journal.Count, new[] { EventHistory.AssignMetadata(marker, JournalStreamId, journal.Count + 1L) }, token);
    private async Task<(IReadOnlyList<IEvent>, Dictionary<OperationKey, OperationJournalEvent>)> ReadAsync(CancellationToken token)
    {
        var journal = await _store.ReadAsync(JournalStreamId, token).ConfigureAwait(false);
        return (journal, OperationJournal.Validate(JournalStreamId, journal));
    }
    private static void Validate(OperationKey key, string version)
    { ArgumentNullException.ThrowIfNull(key); ArgumentException.ThrowIfNullOrWhiteSpace(version); }
    private void ValidateHandle(TransactionHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        if (!ReferenceEquals(handle.Coordinator, this)) throw new OperationIntegrityException("Attempt belongs to another coordinator.");
    }
    private void RequireOwner(TransactionHandle handle, OperationJournalEvent? current)
    {
        if (current is null || current.Kind != OperationJournalKind.Claimed || current.AttemptId != handle.AttemptId
            || current.OwnerSessionId != _store.SessionId)
            throw new OperationIntegrityException("Attempt no longer owns the transaction.");
    }
    private static OperationJournalEvent? Find(Dictionary<OperationKey, OperationJournalEvent> states, OperationKey key, string version)
    {
        var current = states.GetValueOrDefault(key);
        if (current is not null && current.ContractVersion != version)
            throw new OperationIntegrityException("Semantic key was reused with a conflicting contract version.");
        return current;
    }
}

internal static class OperationJournal
{
    internal static Dictionary<OperationKey, OperationJournalEvent> Validate(Guid streamId, IEnumerable<IEvent> history)
    {
        var events = history.ToArray(); EventHistory.Validate(streamId, events);
        var states = new Dictionary<OperationKey, OperationJournalEvent>();
        var attempts = new HashSet<Guid>();
        foreach (var item in events)
        {
            if (item is not OperationJournalEvent e) throw new OperationIntegrityException("Reserved journal contains an unsupported event.");
            var previous = states.GetValueOrDefault(e.Key);
            if (previous is not null && previous.ContractVersion != e.ContractVersion)
                throw new OperationIntegrityException("Operation identity metadata changed in the journal.");
            if (e.Kind == OperationJournalKind.Claimed)
            {
                if ((previous is not null && previous.Kind != OperationJournalKind.RolledBack) || !attempts.Add(e.AttemptId))
                    throw new OperationIntegrityException("Invalid or duplicate operation claim.");
            }
            else if (previous is null || previous.Kind != OperationJournalKind.Claimed
                || previous.AttemptId != e.AttemptId || previous.OwnerSessionId != e.OwnerSessionId)
                throw new OperationIntegrityException("Journal completion has no matching owned claim.");
            if (e.Ranges.Any(r => r.StreamId == streamId)) throw new OperationIntegrityException("Receipt cannot include its own journal.");
            states[e.Key] = e;
        }
        return states;
    }
}
