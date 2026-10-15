using System.Diagnostics;
using System.Text.Json;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Storage.Json;
using EventSourcing.Streams;
using EventSourcing.Transactions;
using EventSourcing.TransactionCrashHost;

namespace EventSourcing.Tests;

public sealed class DurableOperationTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "event-idempotency-" + Guid.NewGuid());
    private string StorePath => Path.Combine(_directory, "events.json");
    private JsonEventStreamStore Open(Action<JsonPublicationPoint>? observer = null) => new(StorePath, CrashProbe.Codec(), observer);
    private static LocalEventTransactionCoordinator Coordinate(JsonEventStreamStore store) => new(store, CrashProbe.Journal);
    private static async Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> History(JsonEventStreamStore store) =>
        await store.ReadBatchAsync(new[] { CrashProbe.Journal, CrashProbe.Source, CrashProbe.Destination });

    [Theory]
    [InlineData("Staged", false)]
    [InlineData("BeforeReplace", false)]
    [InlineData("AfterReplace", true)]
    public async Task Process_death_recovers_complete_batch_or_rollback_and_repeated_restart_is_idempotent(string point, bool committed)
    {
        Directory.CreateDirectory(_directory);
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = true, UseShellExecute = false };
        var testAssembly = typeof(DurableOperationTests).Assembly.Location;
        foreach (var arg in new[] { "exec", "--runtimeconfig", Path.ChangeExtension(testAssembly, ".runtimeconfig.json"),
            "--depsfile", Path.ChangeExtension(testAssembly, ".deps.json"), typeof(CrashProbe).Assembly.Location, StorePath, point }) start.ArgumentList.Add(arg);
        using var child = Process.Start(start)!;
        var errors = child.StandardError.ReadToEndAsync();
        try
        {
            var ready = await child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(ready == "READY", "Crash host failed before fault barrier: " + (child.HasExited ? await errors : ready));
            Assert.Throws<IOException>(() => Open()); // Actual cross-process exclusivity.
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotEqual(0, child.ExitCode);
        }
        finally
        {
            if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
        }
        using (var store = Open())
        {
            var coordinator = Coordinate(store);
            Assert.Equal(committed ? 0 : 1, await coordinator.RecoverAsync());
            Assert.Equal(0, await coordinator.RecoverAsync());
            var history = await History(store);
            Assert.Equal(committed ? 1 : 0, history[CrashProbe.Source].Count);
            Assert.Equal(committed ? 1 : 0, history[CrashProbe.Destination].Count);
            var result = await new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
            Assert.Equal(committed ? OperationStatus.AlreadyConsumed : OperationStatus.Committed, result.Status);
            await AssertFinal(store);
        }
        using (var reopened = Open())
        {
            var coordinator = Coordinate(reopened); Assert.Equal(0, await coordinator.RecoverAsync());
            var duplicate = await new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => throw new Exception("must not execute"));
            Assert.Equal(OperationStatus.AlreadyConsumed, duplicate.Status);
            await AssertFinal(reopened);
        }
    }

    [Theory]
    [InlineData(17, "Staged", false)]
    [InlineData(29, "BeforeReplace", false)]
    [InlineData(43, "AfterReplace", true)]
    public async Task Generated_prefix_survives_abrupt_process_death_without_lost_or_duplicate_effects(int seed, string point, bool published)
    {
        Directory.CreateDirectory(_directory);
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true,
            RedirectStandardInput = true, UseShellExecute = false };
        var assembly = typeof(DurableOperationTests).Assembly.Location;
        foreach (var arg in new[] { "exec", "--runtimeconfig", Path.ChangeExtension(assembly, ".runtimeconfig.json"),
            "--depsfile", Path.ChangeExtension(assembly, ".deps.json"), typeof(CrashProbe).Assembly.Location,
            StorePath, point, seed.ToString(System.Globalization.CultureInfo.InvariantCulture) }) start.ArgumentList.Add(arg);
        using var child = Process.Start(start)!;
        var errors = child.StandardError.ReadToEndAsync();
        try
        {
            var ready = await child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(ready == "READY", $"Seed {seed}, barrier {point}: " + (child.HasExited ? await errors : ready));
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotEqual(0, child.ExitCode);
        }
        finally
        {
            try
            {
                if (!child.HasExited)
                {
                    child.Kill(entireProcessTree: true);
                    await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                }
            }
            catch { /* Preserve the original barrier/test failure. */ }
        }
        using var store = Open();
        var coordinator = Coordinate(store);
        var before = await History(store);
        var completed = published ? 3 : 2;
        Assert.Equal(completed, before[CrashProbe.Source].Count);
        Assert.Equal(completed, before[CrashProbe.Destination].Count);
        Assert.Equal(published ? OperationJournalKind.Committed : OperationJournalKind.Claimed,
            Assert.IsType<OperationJournalEvent>(before[CrashProbe.Journal].Last()).Kind);
        Assert.Equal(published ? 0 : 1, await coordinator.RecoverAsync());
        for (var index = 0; index < 3; index++)
        {
            var captured = index;
            var result = await new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.GeneratedKey(seed, index), "generated/v1",
                _ => Task.FromResult(CrashProbe.GeneratedPlan(seed, captured)));
            Assert.Equal(index < completed ? OperationStatus.AlreadyConsumed : OperationStatus.Committed, result.Status);
        }
        var final = await History(store);
        foreach (var stream in new[] { CrashProbe.Source, CrashProbe.Destination })
        {
            var expected = Enumerable.Range(0, 3).SelectMany(i => CrashProbe.GeneratedPlan(seed, i).Appends.Single(a => a.StreamId == stream).Events)
                .Cast<SyntheticPosting>().ToArray();
            var actual = final[stream].Cast<SyntheticPosting>().ToArray();
            Assert.Equal(expected.Select(e => (e.Id, e.Amount, e.Reason, e.Metadata)), actual.Select(e => (e.Id, e.Amount, e.Reason, e.Metadata)));
        }
        var journal = final[CrashProbe.Journal].Cast<OperationJournalEvent>().ToArray();
        var kinds = new[] { OperationJournalKind.Claimed, OperationJournalKind.Committed,
            OperationJournalKind.Claimed, OperationJournalKind.Committed, OperationJournalKind.Claimed }
            .Concat(published ? new[] { OperationJournalKind.Committed } : new[] {
                OperationJournalKind.RolledBack, OperationJournalKind.Claimed, OperationJournalKind.Committed });
        Assert.Equal(kinds, journal.Select(e => e.Kind));
        for (var index = 0; index < 3; index++)
        {
            var key = CrashProbe.GeneratedKey(seed, index);
            var receipt = Assert.Single(journal, e => e.Key == key && e.Kind == OperationJournalKind.Committed);
            Assert.Equal("generated/v1", receipt.ContractVersion);
            Assert.Equal($"generated-{index}", receipt.Outcome);
            Assert.Equal(journal.Last(e => e.Key == key && e.Kind == OperationJournalKind.Claimed).AttemptId, receipt.AttemptId);
            Assert.Equal(new[] { CrashProbe.Source, CrashProbe.Destination }, receipt.Ranges.Select(r => r.StreamId));
            foreach (var range in receipt.Ranges)
            {
                Assert.Equal(index, range.BeforeVersion);
                Assert.Equal(final[range.StreamId].Skip(index).Take(1).Select(e => e.Id), range.EventIds);
            }
            Assert.Equal(OperationStatus.AlreadyConsumed, (await new OperationProcessor(coordinator).ExecuteAsync(key,
                "generated/v1", _ => throw new Exception("Duplicate generated callback"))).Status);
        }
        Assert.Equal(0, await coordinator.RecoverAsync());
        OperationEvidence.Validate(CrashProbe.Journal, final[CrashProbe.Journal], final);
    }

    private static async Task AssertFinal(JsonEventStreamStore store)
    {
        var history = await History(store);
        var source = Assert.IsType<SyntheticPosting>(Assert.Single(history[CrashProbe.Source]));
        var destination = Assert.IsType<SyntheticPosting>(Assert.Single(history[CrashProbe.Destination]));
        Assert.Equal(CrashProbe.SourceEvent, source.Id); Assert.Equal(-25m, source.Amount); Assert.Equal("source", source.Reason);
        Assert.Equal(CrashProbe.DestinationEvent, destination.Id); Assert.Equal(25m, destination.Amount); Assert.Equal("destination", destination.Reason);
        Assert.Equal(new(1, CrashProbe.Source), source.Metadata); Assert.Equal(new(1, CrashProbe.Destination), destination.Metadata);
        Assert.Equal(0m, source.Amount + destination.Amount);
        var receipt = Assert.Single(history[CrashProbe.Journal].Cast<OperationJournalEvent>(), e => e.Kind == OperationJournalKind.Committed);
        Assert.Equal(CrashProbe.Key, receipt.Key); Assert.Equal("transfer-complete", receipt.Outcome);
        Assert.Equal(new[] { CrashProbe.Source, CrashProbe.Destination }, receipt.Ranges.Select(r => r.StreamId));
        Assert.Equal(new[] { CrashProbe.SourceEvent, CrashProbe.DestinationEvent }, receipt.Ranges.SelectMany(r => r.EventIds));
        var replay = new ReplayOperationAdmission(CrashProbe.Journal, history);
        Assert.Equal(OperationStatus.AlreadyConsumed, replay.Resolve(CrashProbe.Key, "transfer/v1").Status);
    }

    [Fact]
    public void Directory_aliases_share_the_same_exclusive_lease()
    {
        using var store = Open();
        var alias = _directory + "-alias";
        try
        {
            Directory.CreateSymbolicLink(alias, _directory);
            Assert.Throws<IOException>(() => new JsonEventStreamStore(Path.Combine(alias, "events.json"), CrashProbe.Codec()));
        }
        finally { if (Directory.Exists(alias)) Directory.Delete(alias); }
    }

    [Fact]
    public async Task Stale_executor_cannot_commit_after_session_recovery()
    {
        var old = Open(); var oldCoordinator = Coordinate(old);
        var handle = (await oldCoordinator.BeginAsync(CrashProbe.Key, "transfer/v1")).Handle!;
        await oldCoordinator.StageAsync(handle, CrashProbe.Plan()); old.Dispose();
        using var store = Open(); var coordinator = Coordinate(store); Assert.Equal(1, await coordinator.RecoverAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => oldCoordinator.CommitAsync(handle));
        await Assert.ThrowsAsync<OperationIntegrityException>(() => coordinator.CommitAsync(handle));
        Assert.Equal(OperationStatus.Committed, (await new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()))).Status);
        await AssertFinal(store);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Publication_failure_and_lost_acknowledgement_have_distinct_outcomes(bool afterCommit)
    {
        var armed = false;
        using var store = Open(point =>
        { if (armed && (point == JsonPublicationPoint.AfterReplace) == afterCommit) { armed = false; throw new IOException("synthetic publication fault"); } });
        var coordinator = Coordinate(store); var processor = new OperationProcessor(coordinator);
        Task<OperationPlan> Plan(CancellationToken _) { armed = true; return Task.FromResult(CrashProbe.Plan()); }
        if (afterCommit) Assert.Equal(OperationStatus.Committed, (await processor.ExecuteAsync(CrashProbe.Key, "transfer/v1", Plan)).Status);
        else
        {
            await Assert.ThrowsAsync<IOException>(() => processor.ExecuteAsync(CrashProbe.Key, "transfer/v1", Plan));
            Assert.Empty((await History(store))[CrashProbe.Source]);
            Assert.Equal(OperationStatus.RetryAllowed, (await coordinator.ResolveAsync(CrashProbe.Key, "transfer/v1", TimeSpan.Zero)).Status);
            await processor.ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
        }
        Assert.Equal(OperationStatus.AlreadyConsumed, (await processor.ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => throw new Exception("must not execute"))).Status);
        await AssertFinal(store);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Claim_and_rollback_acknowledgement_loss_do_not_leave_unresolvable_owners(bool claim)
    {
        var armed = claim;
        using var store = Open(point =>
        { if (armed && point == JsonPublicationPoint.AfterReplace) { armed = false; throw new IOException("lost marker acknowledgement"); } });
        var coordinator = Coordinate(store);
        var handle = (await coordinator.BeginAsync(CrashProbe.Key, "transfer/v1")).Handle!;
        armed = !claim;
        await coordinator.RollbackAsync(handle);
        Assert.Equal(OperationStatus.RetryAllowed, (await coordinator.ResolveAsync(CrashProbe.Key, "transfer/v1", TimeSpan.Zero)).Status);
        await new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
        await AssertFinal(store);
    }

    [Fact]
    public async Task New_event_IDs_do_not_bypass_durable_semantic_receipts_after_restart()
    {
        using (var original = Open())
            await new OperationProcessor(Coordinate(original)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
        using var store = Open(); var calls = 0;
        var result = await new OperationProcessor(Coordinate(store)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ =>
        {
            calls++;
            return Task.FromResult(new OperationPlan(new[] { new StreamAppend(CrashProbe.Source, 1,
                new[] { new SyntheticPosting(Guid.NewGuid(), -25m, "redelivery", new(2, CrashProbe.Source)) }) }));
        });
        Assert.Equal(OperationStatus.AlreadyConsumed, result.Status); Assert.Equal(0, calls); await AssertFinal(store);
    }

    [Fact]
    public async Task Durable_noop_remains_consumed_after_state_changes_and_restart()
    {
        var key = new OperationKey("synthetic/v1", "no-op");
        using (var first = Open())
        {
            await new OperationProcessor(Coordinate(first)).ExecuteAsync(key, "v1", _ => Task.FromResult(new OperationPlan(Array.Empty<StreamAppend>(), "not-needed")));
            await new OperationProcessor(Coordinate(first)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
        }
        using var store = Open();
        var result = await new OperationProcessor(Coordinate(store)).ExecuteAsync(key, "v1", _ => throw new Exception("changed state must not reinterpret the decision"));
        Assert.Equal(OperationStatus.AlreadyConsumed, result.Status); Assert.Equal("not-needed", result.Receipt!.Outcome);
        Assert.Empty(result.Receipt.Ranges); Assert.Single(await store.ReadAsync(CrashProbe.Source));
    }

    [Fact]
    public async Task Cancellation_before_commit_rolls_back_but_after_publication_does_not_undo()
    {
        using var cancel = new CancellationTokenSource(); var armed = false;
        using (var store = Open(point => { if (armed && point == JsonPublicationPoint.BeforeReplace) { armed = false; cancel.Cancel(); } }))
        {
            var coordinator = Coordinate(store);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new OperationProcessor(coordinator).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ =>
            { armed = true; return Task.FromResult(CrashProbe.Plan()); }, cancellationToken: cancel.Token));
            Assert.Empty((await History(store))[CrashProbe.Source]);
            Assert.Equal(OperationStatus.RetryAllowed, (await coordinator.ResolveAsync(CrashProbe.Key, "transfer/v1", TimeSpan.Zero)).Status);
        }
        using var after = new CancellationTokenSource();
        using (var store = Open(point => { if (armed && point == JsonPublicationPoint.AfterReplace) after.Cancel(); }))
        {
            armed = false;
            Assert.Equal(OperationStatus.Committed, (await new OperationProcessor(Coordinate(store)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ =>
            { armed = true; return Task.FromResult(CrashProbe.Plan()); }, cancellationToken: after.Token)).Status);
            Assert.True(after.IsCancellationRequested); await AssertFinal(store);
        }
    }

    [Fact]
    public async Task Snapshot_suffix_repeated_replay_and_persisted_reload_preserve_facts_and_identity_evidence()
    {
        string before; EventSourcing.Snapshots.AggregateSnapshot<Balance> snapshot;
        var extraId = Guid.NewGuid();
        using (var store = Open())
        {
            await new OperationProcessor(Coordinate(store)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
            var root = EventSourcedAggregate<Balance>.Replay(CrashProbe.Source, new(new Balance(100m), "synthetic/v1"), "synthetic/v1",
                (state, e) => new(state.Amount + ((SyntheticPosting)e).Amount), await store.ReadAsync(CrashProbe.Source));
            snapshot = root.CaptureSnapshot(1);
            await root.SaveSnapshotAsync(new JsonSnapshotStore<Balance>(Path.Combine(_directory, "snapshot.json")), 1);
            await new OperationProcessor(Coordinate(store)).ExecuteAsync(new("synthetic/v1", "extra"), "credit/v1", _ => Task.FromResult(new OperationPlan(new[] {
                new StreamAppend(CrashProbe.Source, 1, new[] { new SyntheticPosting(extraId, 10m, "extra", new(2, CrashProbe.Source)) }) })));
            before = File.ReadAllText(StorePath);
        }
        using var reopened = Open(); var history = await History(reopened);
        for (var iteration = 0; iteration < 3; iteration++)
        {
            var admission = new ReplayOperationAdmission(CrashProbe.Journal, history);
            var replay = EventSourcedAggregate<Balance>.Replay(CrashProbe.Source, new(new Balance(100m), "synthetic/v1"), "synthetic/v1", (state, e) =>
            {
                Assert.Equal(OperationStatus.AlreadyConsumed, admission.Resolve(CrashProbe.Key, "transfer/v1").Status);
                return new(state.Amount + ((SyntheticPosting)e).Amount);
            }, history[CrashProbe.Source]);
            var restored = EventSourcedAggregate<Balance>.Replay(snapshot, "synthetic/v1", 1,
                (state, e) => new(state.Amount + ((SyntheticPosting)e).Amount), await reopened.ReadFromAsync(CrashProbe.Source, snapshot.StreamVersion));
            var loaded = await EventSourcedAggregate<Balance>.LoadAsync(CrashProbe.Source,
                new(new Balance(100m), "synthetic/v1"), "synthetic/v1", 1,
                (state, e) => new(state.Amount + ((SyntheticPosting)e).Amount), reopened,
                new JsonSnapshotStore<Balance>(Path.Combine(_directory, "snapshot.json")));
            Assert.Equal(85m, replay.State.Amount); Assert.Equal(replay.State, restored.State);
            Assert.Equal(replay.State, loaded.State);
            Assert.Equal(extraId, Assert.Single(restored.Events).Id);
            Assert.Equal(before, File.ReadAllText(StorePath));
        }
    }

    [Fact]
    public async Task Old_history_without_keys_remains_replayable_and_no_identity_is_fabricated()
    {
        using var store = Open();
        await store.AppendAsync(CrashProbe.Source, 0, CrashProbe.Plan().Appends[0].Events);
        var view = new ReplayOperationAdmission(CrashProbe.Journal, await History(store));
        Assert.Equal(OperationStatus.ReplaySuppressed, view.Resolve(CrashProbe.Key, "transfer/v1").Status);
        Assert.Empty(await store.ReadAsync(CrashProbe.Journal));
        Assert.Single(await store.ReadAsync(CrashProbe.Source));
    }

    [Theory]
    [InlineData("{\"SchemaVersion\":99,\"Streams\":[]}")]
    [InlineData("{\"SchemaVersion\":1,\"Streams\":null}")]
    [InlineData("invalid json")]
    public void Unsupported_or_corrupt_store_fails_without_replacing_evidence(string text)
    {
        Directory.CreateDirectory(_directory); File.WriteAllText(StorePath, text);
        Assert.ThrowsAny<Exception>(() => Open()); Assert.Equal(text, File.ReadAllText(StorePath));
    }

    [Fact]
    public async Task Corrupt_receipt_and_inconsistent_selected_history_fail_explicitly()
    {
        using (var store = Open())
        {
            await new OperationProcessor(Coordinate(store)).ExecuteAsync(CrashProbe.Key, "transfer/v1", _ => Task.FromResult(CrashProbe.Plan()));
            var history = (await History(store)).ToDictionary(); history[CrashProbe.Destination] = Array.Empty<IEvent>();
            Assert.Throws<OperationIntegrityException>(() => new ReplayOperationAdmission(CrashProbe.Journal, history));
        }
        var text = File.ReadAllText(StorePath).Replace(CrashProbe.SourceEvent.ToString(), Guid.NewGuid().ToString(), StringComparison.Ordinal);
        // Changing only a receipt ID, while leaving the original domain event, must be detected.
        var document = System.Text.Json.Nodes.JsonNode.Parse(text)!;
        var journal = document["Streams"]!.AsArray().Single(s => s!["Id"]!.GetValue<Guid>() == CrashProbe.Journal)!;
        journal["Events"]![1]!["Payload"]!["Ranges"]![0]!["EventIds"]![0] = Guid.NewGuid().ToString();
        var corrupted = document.ToJsonString(); File.WriteAllText(StorePath, corrupted);
        Assert.Throws<OperationIntegrityException>(() => Open()); Assert.Equal(corrupted, File.ReadAllText(StorePath));
    }

    public void Dispose() { if (Directory.Exists(_directory)) Directory.Delete(_directory, true); }
}
