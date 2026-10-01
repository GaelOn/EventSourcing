using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class CloneTypeTests
{
    [Fact]
    public void Type_changing_clone_is_rejected_before_reduction_and_preserves_pending_branch()
    {
        var calls = 0;
        var stream = new EventStream<State, TestEvent>(Guid.NewGuid(), new(0), (s, _) => { calls++; return new(s.Value + 1); })
            .Apply(new Good(Guid.NewGuid()));
        var pending = stream.PendingEvents.ToArray();
        Assert.Throws<EventCloneException>(() => stream.Apply(new Bad(Guid.NewGuid())));
        Assert.Equal(1, calls);
        Assert.Equal(new State(1), stream.State);
        Assert.Equal(1, stream.Version);
        Assert.Equal(0, stream.PersistedVersion);
        Assert.Equal(pending, stream.PendingEvents);
    }
    private sealed record State(int Value);
    private abstract class TestEvent(Guid id, EventMetadata? metadata) : EventBase<TestEvent>(id, metadata ?? new(null, null));
    private sealed class Good(Guid id, EventMetadata? metadata = null) : TestEvent(id, metadata)
    { public override TestEvent Clone(EventMetadata metadata) => new Good(Id, metadata); }
    private sealed class Bad(Guid id) : TestEvent(id, null)
    { public override TestEvent Clone(EventMetadata metadata) => new Good(Id, metadata); }
}
