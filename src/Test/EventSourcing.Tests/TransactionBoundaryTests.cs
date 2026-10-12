using EventSourcing.Streams;
using EventSourcing.Transactions;

namespace EventSourcing.Tests;

public sealed class TransactionBoundaryTests
{
    [Fact]
    public async Task Staging_is_isolated_and_commit_covers_all_streams_and_receipt()
    {
        var store = new InMemoryEventStreamStore(); var journal = Guid.NewGuid();
        var coordinator = new LocalEventTransactionCoordinator(store, journal);
        var key = new OperationKey("synthetic/v1", "transfer/1");
        var admission = await coordinator.BeginAsync(key, "transfer/v1");
        var source = new EventStream(Guid.NewGuid()).Append(new LedgerEvent(Guid.NewGuid(), -25m));
        var destination = new EventStream(Guid.NewGuid()).Append(new LedgerEvent(Guid.NewGuid(), 25m));
        var plan = new OperationPlan(new[] { source, destination }.Select(s => new StreamAppend(s.Id, 0, s.PendingEvents)), "paid");
        await coordinator.StageAsync(admission.Handle!, plan);
        Assert.Empty(await store.ReadAsync(source.Id)); Assert.Empty(await store.ReadAsync(destination.Id));
        var committed = await coordinator.CommitAsync(admission.Handle!);
        Assert.Equal(OperationStatus.Committed, committed.Status);
        Assert.Equal(2, committed.Receipt!.Ranges.Count);
        Assert.Equal(new[] { OperationJournalKind.Claimed, OperationJournalKind.Committed },
            (await store.ReadAsync(journal)).Cast<OperationJournalEvent>().Select(e => e.Kind));
        Assert.Equal(0m, (await store.ReadBatchAsync(new[] { source.Id, destination.Id })).Values.SelectMany(e => e).Cast<LedgerEvent>().Sum(e => e.Amount));
        Assert.Equal(OperationStatus.AlreadyConsumed, (await coordinator.BeginAsync(key, "transfer/v1")).Status);
    }

    [Fact]
    public async Task Stale_participant_publishes_neither_outcome_nor_other_participant()
    {
        var store = new InMemoryEventStreamStore(); var coordinator = new LocalEventTransactionCoordinator(store, Guid.NewGuid());
        var key = new OperationKey("test/v1", "one"); var handle = (await coordinator.BeginAsync(key, "v1")).Handle!;
        var stream = new EventStream(Guid.NewGuid()).Append(new LedgerEvent(Guid.NewGuid(), 1m));
        await stream.SaveAsync(store);
        await coordinator.StageAsync(handle, new(new[] { new StreamAppend(stream.Id, 0, stream.PendingEvents) }));
        await Assert.ThrowsAsync<StreamConcurrencyException>(() => coordinator.CommitAsync(handle));
        await coordinator.RollbackAsync(handle);
        Assert.Single(await store.ReadAsync(stream.Id));
        Assert.Equal(OperationStatus.Claimed, (await coordinator.BeginAsync(key, "v1")).Status);
    }
}
