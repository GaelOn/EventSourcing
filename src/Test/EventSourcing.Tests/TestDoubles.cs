using EventSourcing.ObjectsAggregation;
using EventSourcing.Transaction;

namespace EventSourcing.Tests;

internal sealed class TestEvent(string payload, EventMetadata metadata, Guid? id = null)
    : EventBase<TestEvent>(id ?? Guid.Parse("22222222-2222-2222-2222-222222222222"), metadata)
{
    public string Payload { get; } = payload;
    public static TestEvent Create() => new("Test", new(null, null));
    public override TestEvent Clone(EventMetadata metadata) => new(Payload, metadata, Id);
}

internal sealed class TestSideEffect : ISideEffect
{
    public Func<IApplyStatus> Apply { get; init; } = () => new ApplySuccess();
    public Func<IApplyStatus> Revert { get; init; } = () => new ApplySuccess();
    public int ApplyCalls { get; private set; }
    public int UndoCalls { get; private set; }

    public IApplyStatus ApplySideEffect()
    {
        ApplyCalls++;
        return Apply();
    }

    public IApplyStatus Undo()
    {
        UndoCalls++;
        return Revert();
    }
}

internal sealed record TestState(string Value);

// Keep exercising the supported compatibility API without suppressing warnings in production.
#pragma warning disable CS0618
internal sealed class TestAggregate : EventSourcedAggregate<TestState, TestEvent>
{
    private readonly List<TestEvent> _events = [];
    public TestAggregate(ITransactionResult commitResult, TestSideEffect? sideEffect = null,
        IEventTransactionManager? manager = null, TestState? state = null)
        : base(manager ?? new EventTransactionManager(), state)
    {
        CommitResult = commitResult;
        SideEffect = sideEffect ?? new TestSideEffect();
    }

    public ITransactionResult CommitResult { get; }
    public TestSideEffect SideEffect { get; }
    public TestEvent? PreparedEvent { get; private set; }
    public override Guid Id { get; } = Guid.Parse("ce5f910d-755a-4002-968b-978714c25e36");
    public override IEnumerable<IEntity> Entities => Array.Empty<IEntity>();
    public override IReadOnlyCollection<TestEvent> Events => _events.AsReadOnly();

    protected override ISideEffect PrepareSideEffect(TestEvent @event)
    {
        PreparedEvent = @event;
        return SideEffect;
    }

    protected override ITransactionResult CommitEvent(TestEvent @event)
    {
        if (CommitResult is TransactionSuccess)
            _events.Add(@event);
        return CommitResult;
    }
    protected override ITransactionResult RollbackEvent(ref TestEvent @event)
    {
        _events.Remove(@event);
        return new TransactionRollbacked();
    }
}

#pragma warning restore CS0618
