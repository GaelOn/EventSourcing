using EventSourcing.ObjectsAggregation;

namespace EventSourcing.Transaction;

/// <summary>Legacy synchronous coordinator for commit, effect application and recovery.</summary>
/// <summary>One serialized synchronous commit/application scope with recovery.</summary>
public interface ITransactionScope<in TEvent>
    where TEvent : class, IEvent<TEvent>
{
    /// <summary>Execute the prepared commit and effect, recovering owned resources on failure.</summary>
    /// <summary>Commit and apply one prepared event, recovering failures where possible.</summary>
    ITransactionResult ExecuteTransaction(TEvent @event);
}

/// <summary>Restore owned resources and optionally replace the event during recovery.</summary>
/// <summary>Restore resources changed by commit; changing the local event reference alone is insufficient.</summary>
public delegate ITransactionResult RollbackDelegate<TEvent>(
    ref TEvent @event);

/// <summary>Assign event positions and coordinate the legacy callback lifecycle.</summary>
/// <summary>Assign event positions and coordinate legacy reversible callback scopes.</summary>
public interface IEventTransactionManager
{
    private static readonly EventMetadata NullTransactionData = new(null, null);

    /// <summary>Clone an unrecorded event at the aggregate's next stream position.</summary>
    /// <summary>Clone an unrecorded event with the aggregate identity and next stream position.</summary>
    TEvent GetEventTransactionData<TEvent>(TEvent @event, IAggregate aggregate)
        where TEvent : class, IEvent<TEvent>;

    /// <summary>Create a coordinator for explicit commit, effect and rollback operations.</summary>
    /// <summary>Create a synchronous scope; separate durable commits cannot become atomic through callbacks.</summary>
    public ITransactionScope<TEvent> GetTransactionScope<TEvent>(ISideEffect sideEffect,
        Func<TEvent, ITransactionResult> commit,
        RollbackDelegate<TEvent> rollback)
        where TEvent : class, IEvent<TEvent>
        => new TransactionScope<TEvent>(sideEffect, commit, rollback);

    /// <summary>Clone an event with unassigned metadata for legacy recovery.</summary>
    /// <summary>Clone an event with unassigned metadata, preserving caller identity and payload.</summary>
    public static TEvent ResetEventTransactionData<TEvent>(TEvent @event)
        where TEvent : class, IEvent<TEvent>
        => @event.Clone(NullTransactionData);
}


/// <summary>Default legacy event-position coordinator.</summary>
/// <summary>Default metadata assignment and reversible synchronous transaction coordinator.</summary>
public class EventTransactionManager : IEventTransactionManager
{
    /// <inheritdoc />
    /// <inheritdoc />
    public TEvent GetEventTransactionData<TEvent>(TEvent @event, IAggregate aggregate)
        where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentNullException.ThrowIfNull(aggregate);
        return (TEvent)Streams.EventHistory.AssignMetadata(@event, aggregate.Id, checked(aggregate.Version + 1));
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
