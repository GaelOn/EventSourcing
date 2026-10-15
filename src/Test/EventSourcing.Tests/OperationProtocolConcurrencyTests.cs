using EventSourcing.Streams;
using EventSourcing.Storage.Json;
using EventSourcing.TransactionCrashHost;
using EventSourcing.Transactions;

namespace EventSourcing.Tests;

public sealed class OperationProtocolConcurrencyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Same_key_callers_finish_with_one_effect_batch_and_exact_receipt(bool durable)
    {
        var directory = Path.Combine(Path.GetTempPath(), "protocol-race-" + Guid.NewGuid());
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        ITransactionSessionStore store = durable
            ? new JsonEventStreamStore(Path.Combine(directory, "events.json"), CrashProbe.Codec())
            : new InMemoryEventStreamStore();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbacks = 0;
        var observations = 0;
        Task<OperationPlan> Plan(CancellationToken ct) => Prepare(ct);
        async Task<OperationPlan> Prepare(CancellationToken ct)
        {
            Interlocked.Increment(ref callbacks);
            entered.TrySetResult();
            await finish.Task.WaitAsync(ct);
            return CrashProbe.Plan();
        }
        var calls = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            await start.Task.WaitAsync(deadline.Token);
            var result = await new OperationProcessor(new LocalEventTransactionCoordinator(store, CrashProbe.Journal))
                .ExecuteAsync(CrashProbe.Key, "transfer/v1", Plan, cancellationToken: deadline.Token);
            if (Interlocked.Increment(ref observations) == 7) admitted.TrySetResult();
            return result;
        }, deadline.Token)).ToArray();
        try
        {
            start.SetResult();
            await entered.Task.WaitAsync(deadline.Token);
            // Winner cannot commit until all seven competitors have observed the live claim.
            await admitted.Task.WaitAsync(deadline.Token);
            finish.SetResult();
            var results = await Task.WhenAll(calls).WaitAsync(deadline.Token);
            Assert.Equal(1, callbacks);
            Assert.Single(results, r => r.Status == OperationStatus.Committed);
            Assert.Equal(7, results.Count(r => r.Status == OperationStatus.InProgress));
            var histories = await store.ReadBatchAsync(new[] { CrashProbe.Journal, CrashProbe.Source, CrashProbe.Destination });
            var markers = histories[CrashProbe.Journal].Cast<OperationJournalEvent>().ToArray();
            Assert.Equal(new[] { OperationJournalKind.Claimed, OperationJournalKind.Committed }, markers.Select(e => e.Kind));
            Assert.Equal(markers[0].AttemptId, markers[1].AttemptId);
            Assert.Equal(store.SessionId, markers[1].OwnerSessionId);
            var receipt = Assert.Single(results, r => r.Status == OperationStatus.Committed).Receipt!;
            Assert.Equal(CrashProbe.Key, receipt.Key);
            Assert.Equal("transfer/v1", receipt.ContractVersion);
            Assert.Equal("transfer-complete", receipt.Outcome);
            Assert.Equal(new[] { CrashProbe.Source, CrashProbe.Destination }, receipt.Ranges.Select(r => r.StreamId));
            Assert.Equal(new[] { CrashProbe.SourceEvent, CrashProbe.DestinationEvent }, receipt.Ranges.SelectMany(r => r.EventIds));
            foreach (var range in receipt.Ranges)
            {
                Assert.Equal(0, range.BeforeVersion);
                Assert.Equal(histories[range.StreamId].Select(e => e.Id), range.EventIds);
            }
            Assert.Equal(-25m, Assert.IsType<SyntheticPosting>(Assert.Single(histories[CrashProbe.Source])).Amount);
            Assert.Equal(25m, Assert.IsType<SyntheticPosting>(Assert.Single(histories[CrashProbe.Destination])).Amount);
            var duplicate = await new OperationProcessor(new LocalEventTransactionCoordinator(store, CrashProbe.Journal))
                .ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => throw new Exception("Duplicate callback"));
            Assert.Equal(OperationStatus.AlreadyConsumed, duplicate.Status);
            Assert.Equal(receipt.AttemptId, duplicate.Receipt!.AttemptId);
            OperationEvidence.Validate(CrashProbe.Journal, histories[CrashProbe.Journal], histories);
        }
        finally
        {
            finish.TrySetResult();
            deadline.Cancel();
            try { await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(5)); }
            catch { /* Cleanup must not mask the assertion or bounded-deadline failure. */ }
            (store as IDisposable)?.Dispose();
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }
}
