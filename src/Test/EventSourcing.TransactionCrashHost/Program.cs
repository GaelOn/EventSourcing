using System.Text.Json.Serialization;
using EventSourcing;
using EventSourcing.Streams;
using EventSourcing.Transactions;
using EventSourcing.Storage.Json;

namespace EventSourcing.TransactionCrashHost;

public static class CrashProbe
{
    public static readonly Guid Journal = Guid.Parse("eaac9b36-efcf-4b74-9bde-91a6c054aa11");
    public static readonly Guid Source = Guid.Parse("eaac9b36-efcf-4b74-9bde-91a6c054aa12");
    public static readonly Guid Destination = Guid.Parse("eaac9b36-efcf-4b74-9bde-91a6c054aa13");
    public static readonly Guid SourceEvent = Guid.Parse("eaac9b36-efcf-4b74-9bde-91a6c054aa14");
    public static readonly Guid DestinationEvent = Guid.Parse("eaac9b36-efcf-4b74-9bde-91a6c054aa15");
    public static OperationKey Key => new("synthetic-transfer/v1", "one");
    public static JsonEventCodec Codec() => new JsonEventCodec().Register<SyntheticPosting>("synthetic-posting/v1");
    public static OperationPlan Plan() => new(new[] {
        new StreamAppend(Source, 0, new[] { new SyntheticPosting(SourceEvent, -25m, "source", new(1, Source)) }),
        new StreamAppend(Destination, 0, new[] { new SyntheticPosting(DestinationEvent, 25m, "destination", new(1, Destination)) }) }, "transfer-complete");
    public static async Task<int> Main(string[] args)
    {
        var armed = false;
        using var store = new JsonEventStreamStore(args[0], Codec(), point =>
        {
            if (armed && point.ToString() == args[1]) Block();
        });
        var coordinator = new LocalEventTransactionCoordinator(store, Journal);
        await coordinator.RecoverAsync();
        var handle = (await coordinator.BeginAsync(Key, "transfer/v1")).Handle!;
        await coordinator.StageAsync(handle, Plan());
        if (args[1] == "Staged") Block();
        armed = true;
        await coordinator.CommitAsync(handle);
        return 0;
    }
    private static void Block()
    {
        Console.WriteLine("READY"); Console.Out.Flush();
        Console.ReadLine(); // Parent forcibly terminates this process without Dispose/finally.
    }
}

public sealed class SyntheticPosting : EventBase<SyntheticPosting>
{
    public decimal Amount { get; }
    public string Reason { get; }
    [JsonConstructor]
    public SyntheticPosting(Guid id, decimal amount, string reason, EventMetadata metadata) : base(id, metadata)
    { ArgumentException.ThrowIfNullOrWhiteSpace(reason); Amount = amount; Reason = reason; }
    public override SyntheticPosting Clone(EventMetadata metadata) => new(Id, Amount, Reason, metadata);
}
