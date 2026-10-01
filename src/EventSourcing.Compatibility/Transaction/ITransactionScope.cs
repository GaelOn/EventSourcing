using EventSourcing.ObjectsAggregation;

namespace EventSourcing.Transaction;

public interface ITransactionScope<in TEvent>
    where TEvent : class, IEvent<TEvent>
{
    ITransactionResult ExecuteTransaction(TEvent @event);
}

public delegate ITransactionResult RollbackDelegate<TEvent>(
    ref TEvent @event);

public interface IEventTransactionManager
{
    private static readonly EventMetadata NullTransactionData = new(null, null);

    TEvent GetEventTransactionData<TEvent>(TEvent @event, IAggregate aggregate)
        where TEvent : class, IEvent<TEvent>;

    public ITransactionScope<TEvent> GetTransactionScope<TEvent>(ISideEffect sideEffect,
        Func<TEvent, ITransactionResult> commit,
        RollbackDelegate<TEvent> rollback)
        where TEvent : class, IEvent<TEvent>
        => new TransactionScope<TEvent>(sideEffect, commit, rollback);

    public static TEvent ResetEventTransactionData<TEvent>(TEvent @event)
        where TEvent : class, IEvent<TEvent>
        => @event.Clone(NullTransactionData);
}


public class EventTransactionManager : IEventTransactionManager
{
    public TEvent GetEventTransactionData<TEvent>(TEvent @event, IAggregate aggregate)
        where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentNullException.ThrowIfNull(aggregate);
        return Streams.EventHistory.AssignMetadata(@event, aggregate.Id, checked(aggregate.Version + 1));
    }
}

sealed class TransactionScope<TEvent> : ITransactionScope<TEvent>
    where TEvent : class, IEvent<TEvent>
{
    private readonly object _executionGate = new();
    private bool _executing;
    private readonly ISideEffect sideEffect;
    private readonly Func<TEvent, ITransactionResult> commit;
    private readonly RollbackDelegate<TEvent> rollback;

    public TransactionScope(ISideEffect sideEffect,
        Func<TEvent, ITransactionResult> commit,
        RollbackDelegate<TEvent> rollback)
    {
        this.sideEffect = sideEffect ?? throw new ArgumentNullException(nameof(sideEffect));
        this.commit = commit ?? throw new ArgumentNullException(nameof(commit));
        this.rollback = rollback ?? throw new ArgumentNullException(nameof(rollback));
    }

    public ITransactionResult ExecuteTransaction(TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        lock (_executionGate)
        {
            if (_executing)
            {
                var error = new InvalidOperationException("Reentrant transaction execution is not supported.");
                return new TransactionFailure(error.Message, error);
            }
            _executing = true;
            try { return ExecuteCore(@event); }
            finally { _executing = false; }
        }
    }

    private ITransactionResult ExecuteCore(TEvent @event)
    {
        ITransactionResult committed;
        try
        {
            committed = commit(@event);
        }
        catch (Exception exception)
        {
            // A thrown commit may have made partial changes, unlike an explicit rejection.
            return Recover(@event, new TransactionFailure("Commit threw an exception.", exception), false);
        }

        if (committed is TransactionFailure rejection)
            return rejection;
        if (committed is not TransactionSuccess)
            return Recover(@event, InvalidResult("Commit"), false);

        TransactionFailure failure;
        try
        {
            var applied = sideEffect.ApplySideEffect();
            if (applied is ApplySuccess)
                return new TransactionSuccess();
            failure = applied is ApplyFailure rejected
                ? new TransactionFailure(rejected.Reason, rejected.Exception)
                : InvalidResult("Apply");
        }
        catch (Exception exception)
        {
            failure = new TransactionFailure("Application threw an exception.", exception);
        }
        return Recover(@event, failure, true);
    }

    private TransactionFailure Recover(TEvent @event, TransactionFailure original, bool undoApplication)
    {
        var failures = new List<TransactionFailure> { original };
        if (undoApplication)
        {
            try
            {
                var undone = sideEffect.Undo();
                if (undone is ApplyFailure failure)
                    failures.Add(new TransactionFailure("Undo failed: " + failure.Reason, failure.Exception));
                else if (undone is not ApplySuccess)
                    failures.Add(InvalidResult("Undo"));
            }
            catch (Exception exception)
            {
                failures.Add(new TransactionFailure("Undo threw an exception.", exception));
            }
        }

        // Rollback must still be attempted if undo fails.
        try
        {
            var rolledBack = rollback(ref @event);
            if (rolledBack is TransactionFailure failure)
                failures.Add(failure);
            else if (rolledBack is not TransactionRollbacked)
                failures.Add(InvalidResult("Rollback"));
        }
        catch (Exception exception)
        {
            failures.Add(new TransactionFailure("Rollback threw an exception.", exception));
        }

        return failures.Count == 1 ? original : new TransactionFailure(
            "Recovery incomplete. " + string.Join(" ", failures.Select(failure => failure.Reason)),
            new AggregateException(failures.Select(failure => failure.Exception)));
    }

    private static TransactionFailure InvalidResult(string operation) => new(
        operation + " returned an unsupported or null result.",
        new InvalidOperationException(operation + " violated the transaction result contract."));
}
