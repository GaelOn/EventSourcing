using EventSourcing.Streams;
using EventSourcing.Storage.Json;
using EventSourcing.Transactions;

namespace EventSourcing.Tests;

public sealed class OperationAdmissionTests : IDisposable
{
    private ITransactionSessionStore _store = new InMemoryEventStreamStore();
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "operation-admission-" + Guid.NewGuid());
    private void UseStore(bool durable)
    { if (durable) _store = new JsonEventStreamStore(Path.Combine(_directory, "events.json"), new JsonEventCodec().Register<LedgerEvent>("ledger/v1")); }
    private readonly Guid _journal = Guid.NewGuid();
    private LocalEventTransactionCoordinator Coordinator() => new(_store, _journal);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Concurrent_duplicate_waits_for_commit_and_never_runs_second_consequences(bool durable)
    {
        UseStore(durable);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var key = new OperationKey("synthetic/v1", "noop"); var calls = 0;
        var first = new OperationProcessor(Coordinator()).ExecuteAsync(key, "v1", async _ =>
        { Interlocked.Increment(ref calls); entered.SetResult(); await release.Task; return new(Array.Empty<StreamAppend>(), "no-action"); });
        await entered.Task;
        var second = new OperationProcessor(Coordinator()).ExecuteAsync(key, "v1", _ =>
        { Interlocked.Increment(ref calls); return Task.FromResult(new OperationPlan(Array.Empty<StreamAppend>())); }, TimeSpan.FromSeconds(5));
        Assert.Equal(OperationStatus.InProgress, (await Coordinator().ResolveAsync(key, "v1", TimeSpan.Zero)).Status);
        release.SetResult();
        Assert.Equal(OperationStatus.Committed, (await first).Status);
        var duplicate = await second;
        Assert.Equal(OperationStatus.AlreadyConsumed, duplicate.Status); Assert.Equal("no-action", duplicate.Receipt!.Outcome);
        Assert.Equal(1, calls); Assert.Equal(2, (await _store.ReadAsync(_journal)).Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Waiter_acquires_only_after_failed_isolated_attempt_rolls_back(bool durable)
    {
        UseStore(durable);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var key = new OperationKey("synthetic/v1", "retry"); var streamId = Guid.NewGuid();
        var first = new OperationProcessor(Coordinator()).ExecuteAsync(key, "v1", async _ =>
        {
            var isolated = new EventStream(streamId).Append(new LedgerEvent(Guid.NewGuid(), 10m));
            Assert.Single(isolated.PendingEvents); entered.SetResult(); await release.Task;
            throw new InvalidOperationException("synthetic handler failure");
        });
        await entered.Task;
        var second = new OperationProcessor(Coordinator()).ExecuteAsync(key, "v1", _ =>
        {
            var stream = new EventStream(streamId).Append(new LedgerEvent(Guid.NewGuid(), 10m));
            return Task.FromResult(new OperationPlan(new[] { new StreamAppend(streamId, 0, stream.PendingEvents) }));
        }, TimeSpan.FromSeconds(5));
        Assert.Empty(await _store.ReadAsync(streamId)); release.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => first);
        Assert.Equal(OperationStatus.Committed, (await second).Status);
        Assert.Single(await _store.ReadAsync(streamId));
        Assert.Equal(new[] { OperationJournalKind.Claimed, OperationJournalKind.RolledBack, OperationJournalKind.Claimed, OperationJournalKind.Committed },
            (await _store.ReadAsync(_journal)).Cast<OperationJournalEvent>().Select(e => e.Kind));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Noop_is_consumed_namespaces_are_independent_and_contract_conflicts_fail(bool durable)
    {
        UseStore(durable);
        var processor = new OperationProcessor(Coordinator());
        foreach (var scope in new[] { "tenant-a/v1", "tenant-b/v1" })
            Assert.Equal(OperationStatus.Committed, (await processor.ExecuteAsync(new(scope, "same"), "rules/v1",
                _ => Task.FromResult(new OperationPlan(Array.Empty<StreamAppend>(), "no-op")))).Status);
        Assert.Equal(OperationStatus.AlreadyConsumed, (await processor.ExecuteAsync(new("tenant-a/v1", "same"), "rules/v1",
            _ => throw new Exception("must not execute"))).Status);
        await Assert.ThrowsAsync<OperationIntegrityException>(() => Coordinator().BeginAsync(new("tenant-a/v1", "same"), "rules/v2"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Selected_replay_prefix_never_consults_later_receipts_or_mutates_store(bool durable)
    {
        UseStore(durable);
        var coordinator = Coordinator(); var key = new OperationKey("synthetic/v1", "future");
        var before = await _store.ReadBatchAsync(new[] { _journal });
        await new OperationProcessor(coordinator).ExecuteAsync(key, "v1", _ => Task.FromResult(new OperationPlan(Array.Empty<StreamAppend>())));
        var replay = new ReplayOperationAdmission(_journal, before);
        Assert.Equal(OperationStatus.ReplaySuppressed, replay.Resolve(key, "v1").Status);
        var full = new ReplayOperationAdmission(_journal, await _store.ReadBatchAsync(new[] { _journal }));
        Assert.Equal(OperationStatus.AlreadyConsumed, full.Resolve(key, "v1").Status);
        Assert.Equal(2, (await _store.ReadAsync(_journal)).Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Timeout_and_cancellation_never_grant_a_second_claim(bool durable)
    {
        UseStore(durable);
        var coordinator = Coordinator(); var key = new OperationKey("test/v1", "pending");
        var handle = (await coordinator.BeginAsync(key, "v1")).Handle!;
        Assert.Equal(OperationStatus.InProgress, (await coordinator.ResolveAsync(key, "v1", TimeSpan.Zero)).Status);
        using var cancel = new CancellationTokenSource(); cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => coordinator.ResolveAsync(key, "v1", TimeSpan.FromSeconds(1), cancel.Token));
        Assert.Equal(OperationStatus.InProgress, (await coordinator.BeginAsync(key, "v1")).Status);
        await coordinator.RollbackAsync(handle);
        await Assert.ThrowsAsync<OperationIntegrityException>(() => coordinator.CommitAsync(handle));
    }

    public void Dispose()
    {
        (_store as IDisposable)?.Dispose();
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }

    [Fact]
    public void Invalid_keys_are_rejected_without_invented_identity()
    {
        Assert.Throws<ArgumentException>(() => new OperationKey(" ", "one"));
        Assert.Throws<ArgumentException>(() => new OperationKey("test/v1", ""));
    }
}
