using EventSourcing.Transaction;

namespace EventSourcing.Tests;

public sealed class CallbackEventSourcedAggregateTests
{
    [Fact]
    public void Successful_commit_applies_the_prepared_change_once()
    {
        var aggregate = new TestAggregate(new TransactionSuccess());
        var result = aggregate.AcceptEvent(TestEvent.Create());

        Assert.IsType<TransactionSuccess>(result);
        Assert.Equal(1, aggregate.SideEffect.ApplyCalls);
        Assert.Equal(0, aggregate.SideEffect.UndoCalls);
        Assert.Single(aggregate.Events);
    }

    [Fact]
    public void Acceptance_prepares_and_commits_a_tagged_copy_without_changing_the_callers_event()
    {
        var aggregate = new TestAggregate(new TransactionSuccess());
        var original = TestEvent.Create();
        aggregate.AcceptEvent(original);

        var committed = Assert.Single(aggregate.Events);
        Assert.NotSame(original, committed);
        Assert.Same(aggregate.PreparedEvent, committed);
        Assert.Equal(original.Payload, committed.Payload);
        Assert.Equal(aggregate.Id, committed.Metadata.StreamId);
        Assert.Equal(original.Id, committed.Id);
        Assert.Equal(1, committed.Metadata.Version);
        Assert.Equal(new EventMetadata(null, null), original.Metadata);
    }

    [Fact]
    public void Rejected_commit_does_not_apply_the_prepared_change()
    {
        var rejection = new TransactionFailure("Rejected by storage", new IOException("Storage rejected write"));
        var aggregate = new TestAggregate(rejection);
        var result = aggregate.AcceptEvent(TestEvent.Create());

        Assert.Equal(0, aggregate.SideEffect.ApplyCalls);
        Assert.Empty(aggregate.Events);
        Assert.Same(rejection, result);
    }

    [Fact]
    public void Failure_while_applying_the_change_retains_the_exception_in_the_result()
    {
        var exception = new InvalidOperationException("Cannot apply change");
        var effect = new TestSideEffect { Apply = () => throw exception };
        var aggregate = new TestAggregate(new TransactionSuccess(), effect);

        var result = aggregate.AcceptEvent(TestEvent.Create());

        Assert.Same(exception, Assert.IsType<TransactionFailure>(result).Exception);
        Assert.Empty(aggregate.Events);
        Assert.Equal(1, effect.UndoCalls);
    }

    [Fact]
    public void Supplied_state_is_available_through_the_aggregate()
    {
        var state = new TestState("Opening state");
        var aggregate = new TestAggregate(new TransactionSuccess(), state: state);
        Assert.Same(state, aggregate.State);
    }
    [Fact]
    public async Task Compatibility_aggregate_serializes_concurrent_acceptance()
    {
        var aggregate = new TestAggregate(new TransactionSuccess());
        var events = Enumerable.Range(0, 32).Select(i => new TestEvent(i.ToString(), new(null, null), Guid.NewGuid())).ToArray();
        var results = await Task.WhenAll(events.Select(e => Task.Run(() => aggregate.AcceptEvent(e))));
        Assert.All(results, result => Assert.IsType<TransactionSuccess>(result));
        Assert.Equal(32, aggregate.Version);
        Assert.Equal(32, aggregate.SideEffect.ApplyCalls);
        Assert.Equal(Enumerable.Range(1, 32).Select(i => (long?)i), aggregate.Events.Select(e => e.Metadata.Version));
    }

    [Fact]
    public void Compatibility_aggregate_rejects_reentrant_acceptance()
    {
        TestAggregate? aggregate = null;
        ITransactionResult? nested = null;
        var attempted = false;
        var effect = new TestSideEffect { Apply = () =>
        {
            if (!attempted)
            {
                attempted = true;
                nested = aggregate!.AcceptEvent(new TestEvent("Nested", new(null, null), Guid.NewGuid()));
            }
            return new ApplySuccess();
        } };
        aggregate = new TestAggregate(new TransactionSuccess(), effect);
        Assert.IsType<TransactionSuccess>(aggregate.AcceptEvent(TestEvent.Create()));
        Assert.IsType<TransactionFailure>(nested);
        Assert.Equal(1, aggregate.Version);
    }
}
