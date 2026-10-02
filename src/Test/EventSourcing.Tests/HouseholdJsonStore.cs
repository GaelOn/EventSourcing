using System.Text.Json;
using EventSourcing.ExternalHandlers.Fixture;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

// File-backed acceptance adapter. Discriminators/payload versions are explicit and do not activate CLR type names.
internal sealed class HouseholdJsonStore(string path) : IAtomicEventStreamStore
{
    private readonly JsonFileStore _file = new(path);
    private sealed record Document(int FormatVersion, Definition[] Definitions, StoredStream[] Streams);
    private sealed record Definition(Guid Id, ReplayDefinition<HouseholdData> Opening);
    private sealed record StoredStream(Guid Id, StoredEvent[] Events);
    private sealed record StoredEvent(Guid Id, long Version, string Kind, int PayloadVersion, JsonElement Payload);
    private static Document Empty => new(1, [], []);

    public Task SaveDefinitionAsync(Guid id, ReplayDefinition<HouseholdData> definition) => _file.UpdateAsync(json =>
    {
        var document = DecodeDocument(json);
        if (document.Definitions.Any(d => d.Id == id)) throw new InvalidOperationException("Definition already recorded.");
        return JsonSerializer.Serialize(document with { Definitions = [.. document.Definitions, new(id, definition)] });
    });
    public async Task<ReplayDefinition<HouseholdData>> ReadDefinitionAsync(Guid id) =>
        DecodeDocument(await _file.ReadAsync()).Definitions.Single(d => d.Id == id).Opening;

    public async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default) =>
        await (await ReadStore(cancellationToken)).ReadAsync(streamId, cancellationToken);
    public async Task<StreamSlice> ReadFromAsync(Guid streamId, long afterVersion, CancellationToken cancellationToken = default) =>
        await (await ReadStore(cancellationToken)).ReadFromAsync(streamId, afterVersion, cancellationToken);
    public async Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds, CancellationToken cancellationToken = default) =>
        await (await ReadStore(cancellationToken)).ReadBatchAsync(streamIds, cancellationToken);
    public async Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(IReadOnlyList<StreamReadRequest> requests, CancellationToken cancellationToken = default) =>
        await (await ReadStore(cancellationToken)).ReadBatchFromAsync(requests, cancellationToken);
    public Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<IEvent> events, CancellationToken cancellationToken = default) =>
        AppendBatchAsync([new(streamId, expectedVersion, events)], cancellationToken);
    public Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        var batch = appends.ToArray();
        return _file.UpdateAsync(json =>
        {
            var document = DecodeDocument(json);
            var store = DecodeStore(document);
            store.AppendBatchAsync(batch, cancellationToken).GetAwaiter().GetResult();
            var ids = document.Streams.Select(s => s.Id).Concat(batch.Select(a => a.StreamId)).Distinct().ToArray();
            var histories = store.ReadBatchAsync(ids, cancellationToken).GetAwaiter().GetResult();
            return JsonSerializer.Serialize(document with { Streams = histories.Select(p => new StoredStream(p.Key, p.Value.Select(Encode).ToArray())).ToArray() });
        }, cancellationToken);
    }
    private async Task<InMemoryEventStreamStore> ReadStore(CancellationToken token) => DecodeStore(DecodeDocument(await _file.ReadAsync(token)));
    private static Document DecodeDocument(string? json)
    {
        var document = json is null ? Empty : JsonSerializer.Deserialize<Document>(json) ?? throw new InvalidDataException("Null document.");
        if (document.FormatVersion != 1) throw new UnsupportedEventFormatException("Unsupported ledger format.");
        if (document.Definitions is null || document.Streams is null) throw new InvalidDataException("Missing ledger sections.");
        if (document.Definitions.Any(d => d is null || d.Id == Guid.Empty || d.Opening is null) || document.Definitions.Select(d => d.Id).Distinct().Count() != document.Definitions.Length)
            throw new InvalidDataException("Invalid opening definitions.");
        return document;
    }
    private static InMemoryEventStreamStore DecodeStore(Document document)
    {
        var store = new InMemoryEventStreamStore();
        store.AppendBatchAsync(document.Streams.Select(s => s is null || s.Events is null
            ? throw new InvalidDataException("Missing stream history.") : new StreamAppend(s.Id, 0, s.Events.Select(e => DecodeEvent(s.Id, e)))).ToArray()).GetAwaiter().GetResult();
        return store;
    }
    private static IEvent DecodeEvent(Guid streamId, StoredEvent e)
    {
        if (e is null) throw new InvalidDataException("Null event.");
        if (e.PayloadVersion != 1) throw new UnsupportedEventFormatException("Unsupported payload schema.");
        var metadata = new EventMetadata(e.Version, streamId);
        decimal Amount() => e.Payload.GetProperty("Amount").GetDecimal();
        decimal Value(string name) => e.Payload.GetProperty(name).GetDecimal();
        DateOnly Date() => e.Payload.GetProperty("Date").Deserialize<DateOnly>();
        return e.Kind switch
        {
            "household.income" => new IncomeReceived(e.Id, Amount(), Value("PreviousBalance"), Value("PreviousIncome"), Date(), metadata),
            "household.transfer" => new MoneyTransferred(e.Id, Amount(), Value("PreviousChecking"), Value("PreviousSavings"), Date(), metadata),
            "household.expense" => new ExpensePaid(e.Id, Amount(), Value("PreviousBalance"), Value("PreviousExpense"), Date(), metadata),
            "household.close" => new MonthClosed(e.Id, e.Payload.GetProperty("PreviousCount").GetInt32(), Date(), metadata),
            "household.cancel-income" => new IncomeCancelled(e.Id, e.Payload.GetProperty("OriginalId").GetGuid(), Amount(), metadata),
            "balance.amount" => new LedgerEvent(e.Id, Amount(), metadata),
            _ => throw new UnsupportedEventFormatException("Unknown event discriminator.")
        };
    }
    private static StoredEvent Encode(IEvent e)
    {
        (string kind, object payload) = e switch
        {
            IncomeReceived r => ("household.income", (object)new { r.Amount, r.PreviousBalance, r.PreviousIncome, r.Date }),
            MoneyTransferred r => ("household.transfer", (object)new { r.Amount, r.PreviousChecking, r.PreviousSavings, r.Date }),
            ExpensePaid r => ("household.expense", (object)new { r.Amount, r.PreviousBalance, r.PreviousExpense, r.Date }),
            MonthClosed r => ("household.close", (object)new { r.PreviousCount, r.Date }),
            IncomeCancelled r => ("household.cancel-income", (object)new { r.OriginalId, r.Amount }),
            LedgerEvent r => ("balance.amount", (object)new { r.Amount }),
            _ => throw new UnsupportedEventFormatException("Unmapped event type.")
        };
        return new(e.Id, e.Metadata.Version!.Value, kind, 1, JsonSerializer.SerializeToElement(payload));
    }
}
