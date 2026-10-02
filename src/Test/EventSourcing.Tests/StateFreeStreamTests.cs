using EventSourcing.Streams;
using EventSourcing.ObjectsAggregation;

namespace EventSourcing.Tests;

public sealed class StateFreeStreamTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Invalid_non_generic_clones_fail_before_reduction_and_preserve_the_complete_pending_pair(int mode)
    {
        var calls = 0;
        var root = new EventSourcedAggregate<Counter>(Guid.NewGuid(), new(0), "counter/v1",
            (s, _) => { calls++; return new(s.Value + 1); }).Apply(new First(Guid.NewGuid()));
        var state = root.State;
        var stream = root.Stream;
        var invalid = new InvalidClone(mode == 4 ? Guid.Empty : Guid.NewGuid(), mode);
        if (mode == 4) Assert.Throws<ArgumentException>(() => root.Apply(invalid));
        else Assert.ThrowsAny<InvalidOperationException>(() => root.Apply(invalid));
        Assert.Equal(1, calls);
        Assert.Same(state, root.State); Assert.Same(stream, root.Stream);
        Assert.Equal(1, root.Version); Assert.Equal(0, root.PersistedVersion);
        Assert.Single(root.PendingEvents);
    }

    private sealed record Counter(int Value);
    private sealed class InvalidClone(Guid id, int mode, EventMetadata? metadata = null) : IEvent
    {
        public Guid Id { get; } = id;
        public EventMetadata Metadata { get; } = metadata ?? new(null, null);
        public IEvent Clone(EventMetadata metadata) => mode switch
        {
            0 => null!,
            1 => new InvalidClone(Guid.Empty, mode, metadata),
            2 => new InvalidClone(Id, mode),
            3 => new First(Id, metadata),
            _ => new InvalidClone(Id, mode, metadata)
        };
    }

    [Fact]
    public async Task A_stream_accepts_unrelated_events_without_business_state_and_commits_one_ordered_batch()
    {
        var original = new EventStream(Guid.NewGuid());
        var changed = original.Append(new First(Guid.NewGuid())).Append(new Second(Guid.NewGuid()));
        var store = new InMemoryEventStreamStore();
        var saved = await changed.SaveAsync(store);
        Assert.Empty(original.Events);
        Assert.Equal(2, changed.Version);
        Assert.Equal(new long?[] { 1, 2 }, saved.Events.Select(e => e.Metadata.Version));
        Assert.IsType<First>(saved.Events[0]);
        Assert.IsType<Second>(saved.Events[1]);
        Assert.Equal(2, saved.PersistedVersion);
        Assert.Empty(saved.PendingEvents);
        Assert.Equal(saved.Events, await store.ReadAsync(saved.Id));
        Assert.Null(typeof(EventStream).GetProperty("State"));
        Assert.Null(typeof(EventStream).GetProperty("RuleVersion"));
    }

    private sealed class First(Guid id, EventMetadata? metadata = null) : IEvent
    {
        public Guid Id { get; } = id;
        public EventMetadata Metadata { get; } = metadata ?? new(null, null);
        public IEvent Clone(EventMetadata metadata) => new First(Id, metadata);
    }

    private sealed class Second(Guid id, EventMetadata? metadata = null) : IEvent
    {
        public Guid Id { get; } = id;
        public EventMetadata Metadata { get; } = metadata ?? new(null, null);
        public IEvent Clone(EventMetadata metadata) => new Second(Id, metadata);
    }
}
