using EventSourcing.Streams;
using EventSourcing.Storage.Json;
using EventSourcing.Transactions;

namespace EventSourcing.Tests;

public sealed class TransactionalIdempotencyRegressionTests : IDisposable
{
    private ITransactionSessionStore _store = new InMemoryEventStreamStore();
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "idempotency-regression-" + Guid.NewGuid());
    private void UseStore(bool durable)
    { if (durable) _store = new JsonEventStreamStore(Path.Combine(_directory, "events.json"), new JsonEventCodec().Register<LedgerEvent>("ledger/v1")); }
    private readonly Guid _stream = Guid.NewGuid();

    private readonly Guid _journal = Guid.NewGuid();

    // Semantic identity is deliberately independent of delivery event IDs.
    private async Task Deliver(string operation, Guid deliveryId, decimal amount)
    {
        var processor = new OperationProcessor(new LocalEventTransactionCoordinator(_store, _journal));
        await processor.ExecuteAsync(new("synthetic/v1", operation), "ledger/v1", async _ =>
        {
            var stream = EventStream.Restore(_stream, await _store.ReadAsync(_stream))
                .Append(new LedgerEvent(deliveryId, amount));
            return new(new[] { new StreamAppend(_stream, stream.PersistedVersion, stream.PendingEvents) });
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Same_operation_with_new_event_ID_has_one_committed_effect(bool durable)
    {
        UseStore(durable);
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        var history = await _store.ReadAsync(_stream);
        Assert.Single(history);
        Assert.Equal(100m, history.Cast<LedgerEvent>().Sum(e => e.Amount));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Replay_proposed_threshold_transfer_does_not_duplicate_recorded_consequence(bool durable)
    {
        UseStore(durable);
        await Deliver("income/1", Guid.NewGuid(), 100m);
        await Deliver("threshold/1", Guid.NewGuid(), -25m);
        var selectedHistory = await _store.ReadAsync(_stream);
        var replay = new ReplayOperationAdmission(_journal, await _store.ReadBatchAsync(new[] { _stream, _journal }));
        var journalBefore = (await _store.ReadAsync(_journal)).ToArray();
        // A synthetic threshold rule proposes the same operation while reconstructing its cause.
        foreach (var e in selectedHistory.Cast<LedgerEvent>())
            if (e.Amount >= 100m)
                Assert.Equal(OperationStatus.AlreadyConsumed, replay.Resolve(new("synthetic/v1", "threshold/1"), "ledger/v1").Status);
        Assert.Equal(journalBefore, await _store.ReadAsync(_journal));
        Assert.Equal(2, (await _store.ReadAsync(_stream)).Count);
        Assert.Equal(75m, (await _store.ReadAsync(_stream)).Cast<LedgerEvent>().Sum(e => e.Amount));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Equal_payloads_with_distinct_keys_remain_distinct(bool durable)
    {
        UseStore(durable);
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        await Deliver("credit/2", Guid.NewGuid(), 100m);
        Assert.Equal(2, (await _store.ReadAsync(_stream)).Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Same_event_ID_is_already_rejected_without_second_effect(bool durable)
    {
        UseStore(durable);
        var id = Guid.NewGuid();
        await Deliver("credit/1", id, 100m);
        var saved = EventStream.Restore(_stream, await _store.ReadAsync(_stream));
        Assert.Throws<DuplicateEventException>(() => saved.Append(new LedgerEvent(id, 100m)));
        Assert.Single(await _store.ReadAsync(_stream));
    }
    public void Dispose()
    {
        (_store as IDisposable)?.Dispose();
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }
}
