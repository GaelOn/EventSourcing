using EventSourcing.Transaction;

namespace EventSourcing.Tests;

public sealed class EventTransactionManagerTests
{
    [Fact]
    public void Metadata_assignment_preserves_payload_and_original_metadata()
    {
        IEventTransactionManager manager = new EventTransactionManager();
        var aggregate = new TestAggregate(new TransactionSuccess());
        var original = TestEvent.Create();
        var expectedVersion = aggregate.Version + 1;

        var tagged = manager.GetEventTransactionData(original, aggregate);

        Assert.NotSame(original, tagged);
        Assert.Equal(original.Payload, tagged.Payload);
        Assert.Equal(new EventMetadata(expectedVersion, aggregate.Id), tagged.Metadata);
        Assert.Equal(new EventMetadata(null, null), original.Metadata);
    }

    [Fact]
    public void Reset_returns_an_untagged_copy_and_preserves_the_original()
    {
        var metadata = new EventMetadata(42, Guid.Parse("ce5f910d-755a-4002-968b-978714c25e36"));
        var original = new TestEvent("Payload", metadata);

        var reset = IEventTransactionManager.ResetEventTransactionData(original);

        Assert.NotSame(original, reset);
        Assert.Equal(original.Payload, reset.Payload);
        Assert.Equal(new EventMetadata(null, null), reset.Metadata);
        Assert.Equal(metadata, original.Metadata);
    }

    [Fact]
    public void Repeated_identity_is_rejected_without_applying_again()
    {
        var aggregate = new TestAggregate(new TransactionSuccess());
        var original = TestEvent.Create();
        Assert.IsType<TransactionSuccess>(aggregate.AcceptEvent(original));
        var result = Assert.IsType<TransactionFailure>(aggregate.AcceptEvent(original));
        Assert.IsType<Streams.DuplicateEventException>(result.Exception);
        Assert.Equal(1, aggregate.Version);
        Assert.Equal(1, aggregate.SideEffect.ApplyCalls);
    }
}
