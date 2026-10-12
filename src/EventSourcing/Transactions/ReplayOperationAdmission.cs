using EventSourcing.Streams;

namespace EventSourcing.Transactions;

/// <summary>
/// Immutable identity view of a selected committed history. Historical reduction still applies every recorded
/// event in order; this view only suppresses proposals and cannot append or consult later live history.
/// Reconstruct this evidence from the selected journal when restoring a business-state snapshot.
/// </summary>
public sealed class ReplayOperationAdmission
{
    private readonly Dictionary<OperationKey, OperationJournalEvent> _receipts;
    /// <summary>Create from one consistent selected full-prefix view, including its journal and participant streams.</summary>
    public ReplayOperationAdmission(Guid journalStreamId, IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>> selectedHistory)
    {
        ArgumentNullException.ThrowIfNull(selectedHistory);
        if (!selectedHistory.TryGetValue(journalStreamId, out var journal))
            throw new OperationIntegrityException("Replay requires explicit selected journal evidence, even when empty.");
        foreach (var stream in selectedHistory) EventHistory.Validate(stream.Key, stream.Value);
        var states = OperationJournal.Validate(journalStreamId, journal);
        _receipts = states.Where(pair => pair.Value.Kind == OperationJournalKind.Committed).ToDictionary();
        OperationEvidence.Validate(journalStreamId, journal, selectedHistory);
    }
    /// <summary>Discard recorded consequences; absent proposals are explicitly replay-suppressed, never executed live.</summary>
    public OperationResult Resolve(OperationKey key, string contractVersion)
    {
        ArgumentNullException.ThrowIfNull(key); ArgumentException.ThrowIfNullOrWhiteSpace(contractVersion);
        if (!_receipts.TryGetValue(key, out var receipt)) return new(OperationStatus.ReplaySuppressed);
        if (receipt.ContractVersion != contractVersion) throw new OperationIntegrityException("Replay proposal conflicts with retained identity metadata.");
        return new(OperationStatus.AlreadyConsumed, Receipt: receipt);
    }
}

/// <summary>Storage adapters can verify complete journal protocol and exact committed event-range evidence.</summary>
public static class OperationEvidence
{
    /// <summary>Reject corrupt, incomplete or inconsistently selected receipts without guessing operation identity.</summary>
    public static void Validate(Guid journalStreamId, IReadOnlyList<IEvent> journal,
        IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>> history)
    {
        ArgumentNullException.ThrowIfNull(journal); ArgumentNullException.ThrowIfNull(history);
        OperationJournal.Validate(journalStreamId, journal);
        foreach (var receipt in journal.Cast<OperationJournalEvent>().Where(e => e.Kind == OperationJournalKind.Committed))
            foreach (var range in receipt.Ranges)
            {
                if (!history.TryGetValue(range.StreamId, out var events)
                    || range.BeforeVersion > events.Count || range.EventIds.Count > events.Count - range.BeforeVersion)
                    throw new OperationIntegrityException("Receipt references unavailable committed history.");
                var actual = events.Skip(checked((int)range.BeforeVersion)).Take(range.EventIds.Count).Select(e => e.Id);
                if (!actual.SequenceEqual(range.EventIds)) throw new OperationIntegrityException("Receipt event identities differ from committed history.");
            }
    }
}
