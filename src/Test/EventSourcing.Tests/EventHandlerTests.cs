using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;
using EventSourcing.Snapshots;
using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class EventHandlerTests
{
    private static readonly Guid StreamId = StreamTests.StreamId;
    private static readonly Guid FirstId = StreamTests.FirstId;
    private static readonly Guid SecondId = StreamTests.SecondId;

    [Fact]
    public async Task Typed_handlers_produce_the_same_business_state_live_on_replay_and_after_a_snapshot()
    {
        var handler = new StockHandlers();
        var dispatcher = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Received>(handler).Register<Shipped>(handler).Build();
        var opening = new Stock(10);
        var store = new InMemoryEventStreamStore<StockEvent>();
        var snapshots = new InMemorySnapshotStore<Stock>();
        var original = new EventStream<Stock, StockEvent>(StreamId, opening, dispatcher.Reduce);
        var saved = await original.Apply(new Received(FirstId, 5)).SaveAsync(store);
        await snapshots.SaveAsync(saved.CaptureSnapshot("stock/v1", 1));
        var latest = await saved.Apply(new Shipped(SecondId, 2)).SaveAsync(store);
        var definition = new ReplayDefinition<Stock>(opening, "stock/v1");
        var replayed = EventStream<Stock, StockEvent>.Replay(StreamId, definition,
            "stock/v1", dispatcher.Reduce, await store.ReadAsync(StreamId));
        var loaded = await EventStream<Stock, StockEvent>.LoadAsync(StreamId, definition,
            "stock/v1", 1, dispatcher.Reduce, store, snapshots);

        Assert.Equal(new Stock(13), latest.State);
        Assert.Equal(latest.State, replayed.State);
        Assert.Equal(latest.State, loaded.State);
        Assert.Equal(1, loaded.HistoryStartVersion);
        Assert.Equal(2, loaded.Version);
        Assert.IsType<Shipped>(Assert.Single(loaded.Events));
        Assert.Equal(new Stock(10), original.State);
        Assert.Throws<DuplicateEventException>(() => loaded.Apply(new Received(FirstId, 1)));
    }

    [Fact]
    public async Task Business_root_composes_handlers_and_rejected_events_never_reach_storage()
    {
        var dispatcher = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Received>(new StockHandlers()).Build();
        var root = new StockRoot(new EventStream<Stock, StockEvent>(StreamId, new Stock(10), dispatcher.Reduce));
        var store = new InMemoryEventStreamStore<StockEvent>();
        var savedRoot = new StockRoot(await root.Stream.HandleAsync(new Received(FirstId, 5), store));
        Assert.Equal(new Stock(15), savedRoot.State);
        Assert.Equal(new Stock(10), root.State);
        await Assert.ThrowsAsync<UnhandledEventException>(() =>
            savedRoot.Stream.HandleAsync(new Shipped(SecondId, 2), store));
        Assert.Single(await store.ReadAsync(StreamId));
        Assert.Equal(1, savedRoot.PersistedVersion);
    }

    [Fact]
    public void Duplicate_registration_is_rejected_and_does_not_replace_the_handler()
    {
        var builder = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Received>(new StockHandlers());
        Assert.Throws<ArgumentException>(() => builder.Register<Received>(new StockHandlers()));
        Assert.Equal(new Stock(15), builder.Build().Reduce(new Stock(10), new Received(FirstId, 5)));
    }

    [Fact]
    public void Built_dispatcher_is_unaffected_by_later_builder_registration()
    {
        var builder = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Received>(new StockHandlers());
        var first = builder.Build();
        builder.Register<Shipped>(new StockHandlers());
        var second = builder.Build();
        Assert.Throws<UnhandledEventException>(() => first.Reduce(new Stock(10), new Shipped(FirstId, 2)));
        Assert.Equal(new Stock(8), second.Reduce(new Stock(10), new Shipped(FirstId, 2)));
    }

    [Fact]
    public void Dispatch_uses_exact_runtime_type_and_never_silently_ignores_an_event()
    {
        var dispatcher = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<StockEvent>(new FallbackHandler()).Build();
        var stream = new EventStream<Stock, StockEvent>(StreamId, new Stock(10), dispatcher.Reduce);
        var error = Assert.Throws<UnhandledEventException>(() => stream.Apply(new Received(FirstId, 5)));
        Assert.Equal(typeof(Received), error.EventType);
        Assert.Equal(new Stock(10), stream.State);
        Assert.Empty(stream.PendingEvents);
    }

    [Fact]
    public void Business_failure_preserves_the_original_branch()
    {
        var dispatcher = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Shipped>(new StockHandlers()).Build();
        var stream = new EventStream<Stock, StockEvent>(StreamId, new Stock(1), dispatcher.Reduce);
        Assert.Throws<InvalidOperationException>(() => stream.Apply(new Shipped(FirstId, 2)));
        Assert.Equal(new Stock(1), stream.State);
        Assert.Equal(0, stream.Version);
        Assert.Empty(stream.Events);
    }

    [Fact]
    public void Null_arguments_and_null_handler_results_are_rejected()
    {
        var builder = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder();
        Assert.Throws<ArgumentNullException>(() => builder.Register<Received>(null!));
        var dispatcher = builder.Register<Received>(new NullHandler()).Build();
        Assert.Throws<ArgumentNullException>(() => dispatcher.Reduce(null!, new Received(FirstId, 1)));
        Assert.Throws<ArgumentNullException>(() => dispatcher.Reduce(new Stock(1), null!));
        Assert.Throws<InvalidOperationException>(() => dispatcher.Reduce(new Stock(1), new Received(FirstId, 1)));
    }

    [Fact]
    public void Immutable_dispatcher_can_reduce_independent_states_concurrently()
    {
        var dispatcher = EventHandlerDispatcher<Stock, StockEvent>.CreateBuilder()
            .Register<Received>(new StockHandlers()).Build();
        Parallel.For(0, 100, i => Assert.Equal(new Stock(i + 5),
            dispatcher.Reduce(new Stock(i), new Received(FirstId, 5))));
    }

    private sealed class StockRoot(EventStream<Stock, StockEvent> stream)
        : EventSourcedAggregate<Stock, StockEvent>(stream)
    {
        public override IEnumerable<IEntity> Entities => Array.Empty<IEntity>();
    }
    private sealed record Stock(int Quantity);
    private abstract class StockEvent(Guid id, int quantity, EventMetadata? metadata)
        : EventBase<StockEvent>(id, metadata ?? new(null, null))
    {
        public int Quantity { get; } = quantity;
    }
    private sealed class Received(Guid id, int quantity, EventMetadata? metadata = null)
        : StockEvent(id, quantity, metadata)
    {
        public override StockEvent Clone(EventMetadata metadata) => new Received(Id, Quantity, metadata);
    }
    private sealed class Shipped(Guid id, int quantity, EventMetadata? metadata = null)
        : StockEvent(id, quantity, metadata)
    {
        public override StockEvent Clone(EventMetadata metadata) => new Shipped(Id, Quantity, metadata);
    }
    private sealed class StockHandlers : IEventHandler<Stock, Received>, IEventHandler<Stock, Shipped>
    {
        public Stock Handle(Stock state, Received e) => new(checked(state.Quantity + e.Quantity));
        public Stock Handle(Stock state, Shipped e) => state.Quantity < e.Quantity
            ? throw new InvalidOperationException("Insufficient stock.") : new(state.Quantity - e.Quantity);
    }
    private sealed class FallbackHandler : IEventHandler<Stock, StockEvent>
    {
        public Stock Handle(Stock state, StockEvent e) => state;
    }
    private sealed class NullHandler : IEventHandler<Stock, Received>
    {
        public Stock Handle(Stock state, Received e) => null!;
    }
}
