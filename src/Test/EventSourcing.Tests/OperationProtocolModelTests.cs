using EventSourcing.Storage.Json;
using EventSourcing.Streams;
using EventSourcing.Transactions;
using Xunit.Sdk;
using EventSourcing.TransactionCrashHost;

namespace EventSourcing.Tests;

/// <summary>
/// Model-based check of the claim, stage, commit, rollback and recover protocol. A seeded random walk drives
/// the real coordinator over the durable JSON store, including clean session replacement and
/// lost publication acknowledgements, while a tiny independent model predicts every outcome. After every step
/// the stored evidence must still satisfy the invariants:
/// at most one committed receipt per key, receipts equal to the committed events, business history equal to the
/// committed effects, and a journal state per key equal to the model. A failure prints the seed and the trace.
/// </summary>
public sealed class OperationProtocolModelTests : IDisposable
{
    private const string Contract = "model/v1";
    private const int Steps = 120;
    private static readonly string[] Names = ["k0", "k1", "k2"];
    private static readonly Guid Journal = Guid.Parse("a1a1a1a1-0000-4000-8000-000000000001");
    private static readonly Guid Stream = Guid.Parse("b2b2b2b2-0000-4000-8000-000000000002");

    private enum KeyPhase { None, Claimed, Committed, RolledBack }

    private sealed class KeyModel
    {
        public KeyPhase Phase;
        public int Epoch;                          // storage session that owns a Claimed marker
        public TransactionHandle? Handle;          // capability of the live claim, null once finished
        public TransactionHandle? Last;            // latest handle ever issued, kept to probe fencing
        public int LastEpoch;
        public bool Staged;
        public long StagedExpected;
        public Guid StagedEvent;
        public Guid[] Committed = [];
        public Guid Session;
        public decimal StagedAmount;
    }

    private readonly string _directory = Path.Combine(Path.GetTempPath(), "operation-model-" + Guid.NewGuid());
    private readonly Dictionary<string, KeyModel> _keys = Names.ToDictionary(name => name, _ => new KeyModel());
    private readonly List<string> _trace = [];
    private JsonEventStreamStore? _store;
    private LocalEventTransactionCoordinator? _coordinator;
    private Random _random = new(0);
    private bool _lostAcknowledgement;
    private int _epoch;
    private int _businessCount => _business.Count;
    private readonly List<(Guid Id, decimal Amount, string Reason)> _business = [];
    private readonly List<(string Name, Guid Attempt, Guid Session, OperationJournalKind Kind, long Before, Guid[] Ids)> _journal = [];
    private readonly HashSet<string> _coverage = [];
    private void Marker(string name, KeyModel model, OperationJournalKind kind) =>
        _journal.Add((name, model.Last!.AttemptId, model.Session, kind,
            kind == OperationJournalKind.Committed ? model.StagedExpected : 0,
            kind == OperationJournalKind.Committed ? new[] { model.StagedEvent } : []));

    private string StorePath => Path.Combine(_directory, "events.json");
    private static OperationKey Key(string name) => new("model/v1", name);
    private static JsonEventCodec Codec() => new JsonEventCodec().Register<SyntheticPosting>("model-posting/v1");

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    public async Task Random_interleavings_preserve_the_operation_protocol_invariants(int seed)
    {
        _random = new Random(seed);
        Directory.CreateDirectory(_directory);
        Open();
        try
        {
            await CheckInvariants();
            await EssentialScenarios();
            for (var step = 0; step < Steps; step++)
            {
                await Step();
                _lostAcknowledgement = false;
                await CheckInvariants();
            }
        }
        catch (Exception error)
        {
            throw new XunitException($"Seed {seed} failed after {_trace.Count} steps. Trace: {string.Join(" | ", _trace)}{Environment.NewLine}{error}");
        }
    }

    private async Task EssentialScenarios()
    {
        _trace.Add("deterministic:lost-claim/stale/external/lost-rollback/replacement/recovery/lost-commit");
        await Begin("k0", true);
        await Stage("k0");
        await ExternalAppend();
        await Commit("k0", false);
        _lostAcknowledgement = false;
        await Rollback("k0", true);
        await Begin("k1", false);
        await Stage("k1");
        ReplaceSession();
        await Resolve("k1"); // Explicit recovery has not occurred: still InProgress.
        await Recover();
        await Begin("k2", false);
        await Stage("k2");
        await Commit("k2", true);
        _lostAcknowledgement = false;
        await CheckInvariants();
        Assert.Equal(new[] { "abandoned-recovery", "external", "lost-claim", "lost-commit", "lost-rollback", "replacement-staged", "stale-commit" },
            _coverage.OrderBy(x => x, StringComparer.Ordinal));
    }

    private void Open()
    {
        _store = new JsonEventStreamStore(StorePath, Codec(), point =>
        {
            // The publication is durable but the caller never learns it: the coordinator must resolve it by reading.
            if (_lostAcknowledgement && point == JsonPublicationPoint.AfterReplace)
            {
                _lostAcknowledgement = false;
                throw new IOException("acknowledgement lost");
            }
        });
        _coordinator = new LocalEventTransactionCoordinator(_store, Journal);
    }

    private async Task Step()
    {
        var name = Names[_random.Next(Names.Length)];
        var lost = _random.Next(4) == 0;
        var choice = _random.Next(100);
        var operation = choice switch
        {
            < 22 => "begin",
            < 40 => "stage",
            < 60 => "commit",
            < 70 => "rollback",
            < 78 => "resolve",
            < 86 => "external",
            < 92 => "recover",
            _ => "session-replacement"
        };
        _trace.Add(operation + ":" + name + (lost ? "+lost-ack" : ""));
        switch (operation)
        {
            case "begin": await Begin(name, lost); break;
            case "stage": await Stage(name); break;
            case "commit": await Commit(name, lost); break;
            case "rollback": await Rollback(name, lost); break;
            case "resolve": await Resolve(name); break;
            case "external": await ExternalAppend(); break;
            case "recover": await Recover(); break;
            default: ReplaceSession(); break;
        }
    }

    private bool Live(KeyModel model) => model.Phase == KeyPhase.Claimed && model.Epoch == _epoch && model.Handle is not null;

    private async Task Begin(string name, bool lost)
    {
        var model = _keys[name];
        _lostAcknowledgement = lost;
        var result = await _coordinator!.BeginAsync(Key(name), Contract);
        switch (model.Phase)
        {
            case KeyPhase.Committed:
                Assert.Equal(OperationStatus.AlreadyConsumed, result.Status);
                break;
            case KeyPhase.Claimed:
                // Documents current behavior: a claim whose owner session is dead still reads as in progress
                // until RecoverAsync is called explicitly; timeout never grants ownership.
                Assert.Equal(OperationStatus.InProgress, result.Status);
                break;
            default:
                Assert.Equal(OperationStatus.Claimed, result.Status);
                Assert.NotNull(result.Handle);
                model.Phase = KeyPhase.Claimed;
                model.Epoch = _epoch;
                model.Handle = result.Handle;
                model.Last = result.Handle;
                model.LastEpoch = _epoch;
                model.Staged = false;
                model.Session = _store!.SessionId;
                Marker(name, model, OperationJournalKind.Claimed);
                if (lost) _coverage.Add("lost-claim");
                break;
        }
    }

    private async Task Stage(string name)
    {
        var model = _keys[name];
        var handle = model.Last;
        if (handle is null) return;
        var history = await _store!.ReadAsync(Stream);
        var stream = EventStream.Restore(Stream, history).Append(new SyntheticPosting(Guid.NewGuid(), _random.Next(1, 100), "model", new(null, null)));
        var plan = new OperationPlan(new[] { new StreamAppend(Stream, stream.PersistedVersion, stream.PendingEvents) });
        if (!Live(model) || model.Staged)
        {
            // Stale session, lost ownership or a second staging: always an integrity failure.
            await Assert.ThrowsAsync<OperationIntegrityException>(() => _coordinator!.StageAsync(handle, plan));
            return;
        }
        await _coordinator!.StageAsync(handle, plan);
        model.Staged = true;
        Assert.Equal(_business.Count, stream.PersistedVersion);
        model.StagedExpected = _business.Count;
        model.StagedEvent = stream.PendingEvents[0].Id;
        model.StagedAmount = ((SyntheticPosting)stream.PendingEvents[0]).Amount;
    }

    private async Task Commit(string name, bool lost)
    {
        var model = _keys[name];
        var handle = model.Last;
        if (handle is null) return;
        if (!Live(model))
        {
            if (model.LastEpoch != _epoch)
                await Assert.ThrowsAsync<OperationIntegrityException>(() => _coordinator!.CommitAsync(handle));
            else if (model.Phase == KeyPhase.Committed)
                Assert.Equal(OperationStatus.AlreadyConsumed, (await _coordinator!.CommitAsync(handle)).Status);
            else
                await Assert.ThrowsAsync<OperationIntegrityException>(() => _coordinator!.CommitAsync(handle));
            return;
        }
        if (!model.Staged)
        {
            await Assert.ThrowsAsync<OperationIntegrityException>(() => _coordinator!.CommitAsync(handle));
            return;
        }
        _lostAcknowledgement = lost;
        if (model.StagedExpected != _businessCount)
        {
            _coverage.Add("stale-commit");
            // A stale participant publishes nothing; the claim stays owned and staged until an explicit rollback.
            await Assert.ThrowsAsync<StreamConcurrencyException>(() => _coordinator!.CommitAsync(handle));
            return;
        }
        var result = await _coordinator!.CommitAsync(handle);
        Assert.Equal(OperationStatus.Committed, result.Status);
        model.Phase = KeyPhase.Committed;
        model.Handle = null;
        model.Staged = false;
        model.Committed = [model.StagedEvent];
        _business.Add((model.StagedEvent, model.StagedAmount, "model"));
        Marker(name, model, OperationJournalKind.Committed);
        if (lost) _coverage.Add("lost-commit");
    }

    private async Task Rollback(string name, bool lost)
    {
        var model = _keys[name];
        var handle = model.Last;
        if (handle is null) return;
        if (model.LastEpoch != _epoch)
        {
            await Assert.ThrowsAsync<OperationIntegrityException>(() => _coordinator!.RollbackAsync(handle));
            return;
        }
        _lostAcknowledgement = lost;
        // For a finished attempt this is an idempotent no-op that never undoes a commit.
        await _coordinator!.RollbackAsync(handle);
        if (Live(model))
        {
            Marker(name, model, OperationJournalKind.RolledBack);
            if (lost) _coverage.Add("lost-rollback");
            model.Phase = KeyPhase.RolledBack;
            model.Handle = null;
            model.Staged = false;
        }
    }

    private async Task Resolve(string name)
    {
        var result = await _coordinator!.ResolveAsync(Key(name), Contract, TimeSpan.Zero);
        var expected = _keys[name].Phase switch
        {
            KeyPhase.Committed => OperationStatus.AlreadyConsumed,
            KeyPhase.Claimed => OperationStatus.InProgress,
            _ => OperationStatus.RetryAllowed
        };
        Assert.Equal(expected, result.Status);
    }

    private async Task ExternalAppend()
    {
        // A writer outside the operation protocol moves the business stream, making staged plans stale.
        var history = await _store!.ReadAsync(Stream);
        var posting = new SyntheticPosting(Guid.NewGuid(), _random.Next(1, 100), "external", new(null, null));
        await EventStream.Restore(Stream, history).Append(posting).SaveAsync(_store);
        _business.Add((posting.Id, posting.Amount, posting.Reason));
        _coverage.Add("external");
    }

    private async Task Recover()
    {
        var abandoned = _keys.Values.Where(model => model.Phase == KeyPhase.Claimed && model.Epoch != _epoch).ToArray();
        Assert.Equal(abandoned.Length, await _coordinator!.RecoverAsync());
        foreach (var model in abandoned)
        {
            var name = _keys.Single(pair => ReferenceEquals(pair.Value, model)).Key;
            Marker(name, model, OperationJournalKind.RolledBack);
            _coverage.Add("abandoned-recovery");
            model.Phase = KeyPhase.RolledBack;
            model.Handle = null;
            model.Staged = false;
        }
    }

    private void ReplaceSession()
    {
        // Releasing the lease and reopening the same file is a new storage session: every handle becomes stale
        // and claims of the previous session stay in the journal until RecoverAsync resolves them.
        if (_keys.Values.Any(m => Live(m) && m.Staged)) _coverage.Add("replacement-staged");
        _store!.Dispose();
        _epoch++;
        Open();
    }

    private async Task CheckInvariants()
    {
        var history = await _store!.ReadBatchAsync(new[] { Journal, Stream });
        var journal = history[Journal];
        Assert.Equal(_business, history[Stream].Cast<SyntheticPosting>().Select(e => (e.Id, e.Amount, e.Reason)));
        Assert.Equal(_journal.Count, journal.Count);
        Assert.Equal(journal.Count, journal.Select(e => e.Id).Distinct().Count());
        for (var index = 0; index < journal.Count; index++)
        {
            var actual = Assert.IsType<OperationJournalEvent>(journal[index]);
            var expected = _journal[index];
            Assert.NotEqual(Guid.Empty, actual.Id);
            Assert.Equal(new EventMetadata(index + 1, Journal), actual.Metadata);
            Assert.Equal(Key(expected.Name), actual.Key);
            Assert.Equal(Contract, actual.ContractVersion);
            Assert.Equal(expected.Attempt, actual.AttemptId);
            Assert.Equal(expected.Session, actual.OwnerSessionId);
            Assert.Equal(expected.Kind, actual.Kind);
            Assert.Null(actual.Outcome);
            if (expected.Kind == OperationJournalKind.Committed)
            {
                var range = Assert.Single(actual.Ranges);
                Assert.Equal(Stream, range.StreamId);
                Assert.Equal(expected.Before, range.BeforeVersion);
                Assert.Equal(expected.Ids, range.EventIds);
            }
            else Assert.Empty(actual.Ranges);
        }
        for (var index = 0; index < history[Stream].Count; index++)
            Assert.Equal(new EventMetadata(index + 1, Stream), history[Stream][index].Metadata);
        OperationEvidence.Validate(Journal, journal, history);

        var events = journal.Cast<OperationJournalEvent>().ToArray();
        Assert.All(events.Where(e => e.Kind == OperationJournalKind.Committed).GroupBy(e => e.Key.Value), group => Assert.Single(group));

        var last = new Dictionary<string, OperationJournalEvent>();
        foreach (var e in events) last[e.Key.Value] = e;
        foreach (var (name, model) in _keys)
        {
            OperationJournalKind? expected = model.Phase switch
            {
                KeyPhase.Claimed => OperationJournalKind.Claimed,
                KeyPhase.Committed => OperationJournalKind.Committed,
                KeyPhase.RolledBack => OperationJournalKind.RolledBack,
                _ => null
            };
            last.TryGetValue(name, out var actual);
            Assert.Equal(expected, actual?.Kind);
            if (model.Phase != KeyPhase.Committed) continue;
            var range = Assert.Single(actual!.Ranges);
            Assert.Equal(Stream, range.StreamId);
            Assert.True(model.Committed.SequenceEqual(range.EventIds), "Receipt range differs from the committed events.");
        }

        var business = history[Stream];
        Assert.Equal(_businessCount, business.Count);
        Assert.Equal(business.Count, business.Select(e => e.Id).Distinct().Count());
        foreach (var model in _keys.Values.Where(m => m.Phase == KeyPhase.Committed))
            Assert.Contains(model.Committed[0], business.Select(e => e.Id));
    }

    public void Dispose()
    {
        _store?.Dispose();
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }
}
