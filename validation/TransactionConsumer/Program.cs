using System.Text.Json.Serialization;
using EventSourcing;
using EventSourcing.Streams;
using EventSourcing.Transactions;
using EventSourcing.Storage.Json;

var directory = Path.Combine(Path.GetTempPath(), "transaction-consumer-" + Guid.NewGuid());
var path = Path.Combine(directory, "events.json");
var journal = Guid.NewGuid(); var source = Guid.NewGuid(); var destination = Guid.NewGuid();
var key = new OperationKey("synthetic-ledger/tenant-1/v1", "transfer-1");
var codec = new JsonEventCodec().Register<Posting>("posting/v1");
try
{
    using (var store = new JsonEventStreamStore(path, codec))
    {
        var coordinator = new LocalEventTransactionCoordinator(store, journal);
        await coordinator.RecoverAsync();
        var processor = new OperationProcessor(coordinator);
        var result = await processor.ExecuteAsync(key, "transfer/v1", _ =>
        {
            var from = new EventStream(source).Append(new Posting(Guid.NewGuid(), -25m, "source"));
            var to = new EventStream(destination).Append(new Posting(Guid.NewGuid(), 25m, "destination"));
            return Task.FromResult(new OperationPlan(new[] { from, to }.Select(s =>
                new StreamAppend(s.Id, s.PersistedVersion, s.PendingEvents)), "transferred"));
        });
        Require(result.Status == OperationStatus.Committed && result.Receipt!.Ranges.Count == 2);
        var duplicate = await processor.ExecuteAsync(key, "transfer/v1", _ => throw new Exception("Duplicate consequences executed."));
        Require(duplicate.Status == OperationStatus.AlreadyConsumed && duplicate.Receipt!.Outcome == "transferred");
    }
    using (var reopened = new JsonEventStreamStore(path, codec))
    {
        var coordinator = new LocalEventTransactionCoordinator(reopened, journal);
        Require(await coordinator.RecoverAsync() == 0);
        var duplicate = await new OperationProcessor(coordinator).ExecuteAsync(key, "transfer/v1", _ => throw new Exception("Reload lost consumed identity."));
        Require(duplicate.Status == OperationStatus.AlreadyConsumed);
        var history = await reopened.ReadBatchAsync(new[] { journal, source, destination });
        Require(history[source].Count == 1 && history[destination].Count == 1);
        Require(history[source].Cast<Posting>().Sum(e => e.Amount) + history[destination].Cast<Posting>().Sum(e => e.Amount) == 0m);
        var replay = new ReplayOperationAdmission(journal, history);
        Require(replay.Resolve(key, "transfer/v1").Status == OperationStatus.AlreadyConsumed);
        Require(replay.Resolve(new(key.Namespace, "absent"), "transfer/v1").Status == OperationStatus.ReplaySuppressed);
    }
    Console.WriteLine("Transaction NuGet consumer passed: atomic transfer, duplicate discard, persisted reload and replay admission.");
}
finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
static void Require(bool condition) { if (!condition) throw new Exception("Transaction consumer check failed."); }
public sealed class Posting : EventBase<Posting>
{
    public decimal Amount { get; }
    public string Reason { get; }
    [JsonConstructor]
    public Posting(Guid id, decimal amount, string reason, EventMetadata? metadata = null) : base(id, metadata ?? new(null, null))
    { Amount = amount; Reason = reason; }
    public override Posting Clone(EventMetadata metadata) => new(Id, Amount, Reason, metadata);
}
