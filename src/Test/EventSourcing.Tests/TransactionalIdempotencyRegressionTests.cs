using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class TransactionalIdempotencyRegressionTests
{
    private readonly InMemoryEventStreamStore _store = new();
    private readonly Guid _stream = Guid.NewGuid();

    // Semantic identity is deliberately independent of delivery event IDs.
    private async Task Deliver(string operation, Guid deliveryId, decimal amount)
    {
        _ = operation;
        var stream = EventStream.Restore(_stream, await _store.ReadAsync(_stream));
        await stream.Append(new LedgerEvent(deliveryId, amount)).SaveAsync(_store);
    }

    [Fact]
    public async Task Same_operation_with_new_event_ID_has_one_committed_effect()
    {
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        var history = await _store.ReadAsync(_stream);
        Assert.Single(history);
        Assert.Equal(100m, history.Cast<LedgerEvent>().Sum(e => e.Amount));
    }

    [Fact]
    public async Task Replay_proposed_threshold_transfer_does_not_duplicate_recorded_consequence()
    {
        await Deliver("income/1", Guid.NewGuid(), 100m);
        await Deliver("threshold/1", Guid.NewGuid(), -25m);
        var selectedHistory = await _store.ReadAsync(_stream);
        // A synthetic threshold rule proposes the same operation while reconstructing its cause.
        foreach (var e in selectedHistory.Cast<LedgerEvent>())
            if (e.Amount >= 100m) await Deliver("threshold/1", Guid.NewGuid(), -25m);
        Assert.Equal(2, (await _store.ReadAsync(_stream)).Count);
        Assert.Equal(75m, (await _store.ReadAsync(_stream)).Cast<LedgerEvent>().Sum(e => e.Amount));
    }

    [Fact]
    public async Task Equal_payloads_with_distinct_keys_remain_distinct()
    {
        await Deliver("credit/1", Guid.NewGuid(), 100m);
        await Deliver("credit/2", Guid.NewGuid(), 100m);
        Assert.Equal(2, (await _store.ReadAsync(_stream)).Count);
    }

    [Fact]
    public async Task Same_event_ID_is_already_rejected_without_second_effect()
    {
        var id = Guid.NewGuid();
        await Deliver("credit/1", id, 100m);
        await Assert.ThrowsAsync<DuplicateEventException>(() => Deliver("credit/1", id, 100m));
        Assert.Single(await _store.ReadAsync(_stream));
    }
}
