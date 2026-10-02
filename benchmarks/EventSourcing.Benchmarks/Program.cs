using EventSourcing.ObjectsAggregation;
using System.Diagnostics;
using System.Globalization;
using EventSourcing;
using EventSourcing.Streams;

// A repeatable diagnostic, not a timing assertion. IDs are prepared outside the measured operation.
Console.WriteLine("events,operation,elapsed_ms,allocated_bytes");
await Measure(100, report: false);
foreach (var count in new[] { 1_000, 5_000, 10_000 })
    await Measure(count, report: true);

static async Task Measure(int count, bool report)
{
    var events = Enumerable.Range(0, count).Select(_ => new AmountEvent(Guid.NewGuid(), 1m)).ToArray();
    foreach (var persist in new[] { false, true })
    {
        var stream = new EventSourcedAggregate<Balance>(Guid.NewGuid(), new(0m),
            new EventReducer<Balance>("balance/v1", (state, e) => new(state.Amount + ((AmountEvent)e).Amount)));
        var store = new InMemoryEventStreamStore();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetTotalAllocatedBytes(precise: true);
        var timer = Stopwatch.StartNew();
        foreach (var e in events)
        {
            stream = stream.Apply(e);
            if (persist) stream = await stream.SaveAsync(store);
        }
        timer.Stop();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        if (stream.State.Amount != count || stream.Version != count)
            throw new InvalidOperationException("Benchmark did not reconcile.");
        if (persist && (await store.ReadAsync(stream.Id)).Count != count)
            throw new InvalidOperationException("Benchmark did not persist all events.");
        if (report)
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"{count},{(persist ? "apply-and-save" : "apply")},{timer.Elapsed.TotalMilliseconds:F2},{allocated}"));
    }
}

sealed record Balance(decimal Amount);

sealed class AmountEvent(Guid id, decimal amount, EventMetadata? metadata = null)
    : EventBase<AmountEvent>(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public override AmountEvent Clone(EventMetadata metadata) => new(Id, Amount, metadata);
}
