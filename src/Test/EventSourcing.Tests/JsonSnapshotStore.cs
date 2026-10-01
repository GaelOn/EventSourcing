using System.Text.Json;
using EventSourcing.Snapshots;

namespace EventSourcing.Tests;

/// <summary>Test-only checkpoint cache, separate from the authoritative event ledger.</summary>
internal sealed class JsonSnapshotStore<TState>(string path) : ISnapshotStore<TState> where TState : class
{
    private readonly JsonFileStore _file = new(path);

    public async Task<AggregateSnapshot<TState>?> ReadAsync(Guid aggregateId, string ruleVersion, int schemaVersion,
        CancellationToken cancellationToken = default)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("An aggregate ID is required.", nameof(aggregateId));
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion));
        var records = Decode(await _file.ReadAsync(cancellationToken));
        var record = records.SingleOrDefault(s => s.AggregateId == aggregateId &&
            s.RuleVersion == ruleVersion && s.SchemaVersion == schemaVersion);
        if (record is null) return null;
        // Incompatible schema payloads need not deserialize as the current business-state type.
        var state = record.State.Deserialize<TState>() ?? throw new InvalidDataException("Snapshot state is null.");
        return new(record.AggregateId, record.StreamVersion, state, record.RuleVersion, record.SchemaVersion);
    }

    public Task SaveAsync(AggregateSnapshot<TState> snapshot, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return _file.UpdateAsync(json =>
        {
            var records = Decode(json);
            var index = records.FindIndex(s => s.AggregateId == snapshot.AggregateId &&
                s.RuleVersion == snapshot.RuleVersion && s.SchemaVersion == snapshot.SchemaVersion);
            if (index < 0 || records[index].StreamVersion < snapshot.StreamVersion)
            {
                var record = new SnapshotRecord(snapshot.AggregateId, snapshot.StreamVersion,
                    snapshot.RuleVersion, snapshot.SchemaVersion, JsonSerializer.SerializeToElement(snapshot.State));
                if (index < 0) records.Add(record);
                else records[index] = record;
            }
            return JsonSerializer.Serialize(new SnapshotDocument(1, records.ToArray()));
        }, cancellationToken);
    }

    private static List<SnapshotRecord> Decode(string? json)
    {
        if (json is null) return [];
        var document = JsonSerializer.Deserialize<SnapshotDocument>(json)
            ?? throw new InvalidDataException("Snapshot document is null.");
        if (document.FormatVersion != 1 || document.Snapshots is null)
            throw new InvalidDataException("Snapshot document format is invalid.");
        var keys = new HashSet<(Guid, string, int)>();
        foreach (var record in document.Snapshots)
        {
            if (record is null || record.AggregateId == Guid.Empty || record.StreamVersion < 0 ||
                string.IsNullOrWhiteSpace(record.RuleVersion) || record.SchemaVersion < 1 ||
                record.State.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw new InvalidDataException("Snapshot record is invalid.");
            if (!keys.Add((record.AggregateId, record.RuleVersion, record.SchemaVersion)))
                throw new InvalidDataException("Duplicate snapshot cache key.");
        }
        return document.Snapshots.ToList();
    }

    private sealed record SnapshotRecord(Guid AggregateId, long StreamVersion,
        string RuleVersion, int SchemaVersion, JsonElement State);
    private sealed record SnapshotDocument(int FormatVersion, SnapshotRecord[] Snapshots);
}
