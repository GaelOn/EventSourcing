using EventSourcing.Streams;
using EventSourcing.Transaction;

namespace EventSourcing.Tests;

public sealed class StreamTests
{
    internal static readonly Guid StreamId = Guid.Parse("11111111-1111-1111-1111-111111111111");
    internal static readonly Guid FirstId = Guid.Parse("22222222-2222-2222-2222-222222222222");
    internal static readonly Guid SecondId = Guid.Parse("33333333-3333-3333-3333-333333333333");
    internal static EventStream<Balance, LedgerEvent> Create() => new(StreamId, new Balance(100m), Reduce);
    internal static Balance Reduce(Balance state, LedgerEvent @event) => new(state.Amount + @event.Amount);

    [Fact]
    public void Identity_is_preserved_while_versions_follow_acceptance_order()
    {
        var original = Create();
        var income = new LedgerEvent(FirstId, 25m);
        var changed = original.Apply(income).Apply(new LedgerEvent(SecondId, -10m));

        Assert.Equal(100m, original.State.Amount);
        Assert.Equal(115m, changed.State.Amount);
        Assert.Equal(2, changed.Version);
        Assert.Equal(0, changed.PersistedVersion);
        Assert.Equal(new[] { FirstId, SecondId }, changed.Events.Select(e => e.Id));
        Assert.Equal(new long?[] { 1, 2 }, changed.Events.Select(e => e.TransactionnalData.Version));
        Assert.All(changed.Events, e => Assert.Equal(StreamId, e.TransactionnalData.AggregateId));
        Assert.Null(income.TransactionnalData.Version);
    }

    [Fact]
    public void Duplicate_identity_cannot_apply_a_second_effect_even_with_different_payload()
    {
        var stream = Create().Apply(new LedgerEvent(FirstId, 25m));
        Assert.Throws<DuplicateEventException>(() => stream.Apply(new LedgerEvent(FirstId, 999m)));
        Assert.Equal(125m, stream.State.Amount);
        Assert.Single(stream.Events);
    }

    [Fact]
    public void Replay_matches_live_state_without_invoking_the_transaction_manager_or_store()
    {
        var live = Create().Apply(new LedgerEvent(FirstId, 25m)).Apply(new LedgerEvent(SecondId, -10m));
        var calls = 0;
        var replayed = EventStream<Balance, LedgerEvent>.Replay(StreamId, new Balance(100m),
            (state, e) => { calls++; return Reduce(state, e); }, live.Events);

        Assert.Equal(live.State, replayed.State);
        Assert.Equal(2, calls);
        Assert.Equal(2, replayed.PersistedVersion);
        Assert.Empty(replayed.PendingEvents);
        Assert.Throws<DuplicateEventException>(() => replayed.Apply(new LedgerEvent(FirstId, 1m)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void Replay_rejects_gaps_wrong_streams_and_duplicate_ids(int invalidCase)
    {
        var first = new LedgerEvent(FirstId, 25m, new(1, StreamId));
        var second = invalidCase switch
        {
            0 => new LedgerEvent(SecondId, 1m, new(3, StreamId)),
            1 => new LedgerEvent(SecondId, 1m, new(2, SecondId)),
            _ => new LedgerEvent(FirstId, 1m, new(2, StreamId))
        };
        Assert.ThrowsAny<InvalidOperationException>(() =>
            EventStream<Balance, LedgerEvent>.Replay(StreamId, new Balance(100m), Reduce, [first, second]));
    }

    [Fact]
    public void Reducer_failure_leaves_original_state_and_history_unchanged()
    {
        var stream = new EventStream<Balance, LedgerEvent>(StreamId, new Balance(100m),
            (state, e) => throw new InvalidOperationException("Rejected"));
        Assert.Throws<InvalidOperationException>(() => stream.Apply(new LedgerEvent(FirstId, 1m)));
        Assert.Equal(100m, stream.State.Amount);
        Assert.Empty(stream.Events);
    }
    [Fact]
    public void Empty_event_identity_is_rejected_at_construction()
    {
        Assert.Throws<ArgumentException>(() => new LedgerEvent(Guid.Empty, 10m));
    }

    [Fact]
    public void Recorded_events_cannot_be_reassigned_to_another_stream()
    {
        var recorded = Assert.Single(Create().Apply(new LedgerEvent(FirstId, 10m)).Events);
        var another = new EventStream<Balance, LedgerEvent>(SecondId, new Balance(0m), Reduce);
        Assert.Throws<InvalidOperationException>(() => another.Apply(recorded));
        Assert.Empty(another.Events);
    }

}

public sealed record Balance(decimal Amount);

public sealed class LedgerEvent : EventBase<LedgerEvent>
{
    public decimal Amount { get; }
    public LedgerEvent(Guid id, decimal amount, TransactionnalData? transactionnalData = null)
        : base(id, transactionnalData ?? new(null, null)) => Amount = amount;
    public override LedgerEvent Clone(TransactionnalData transactionnalData) => new(Id, Amount, transactionnalData);
}
