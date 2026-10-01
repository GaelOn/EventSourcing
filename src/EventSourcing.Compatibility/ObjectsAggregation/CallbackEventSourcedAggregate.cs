using EventSourcing.Streams;
using EventSourcing.Transaction;

namespace EventSourcing.ObjectsAggregation;

/// <summary>Legacy synchronous reversible callback lifecycle. Business aggregate roots remain a supported core concept.</summary>
[Obsolete("Use the core EventSourcedAggregate composing an EventStream for new business roots. This callback lifecycle is retained for compatibility.")]
public abstract class CallbackEventSourcedAggregate<TState, TEvent>
    : ICallbackEventSourcedAggregate<TState, TEvent>
    where TState : class
    where TEvent : class, IEvent<TEvent>
{
    private readonly object _acceptGate = new();
    private bool _accepting;
    protected readonly IEventTransactionManager _eventTransactionManager;

    public abstract Guid Id { get; }
    public abstract IEnumerable<IEntity> Entities { get; }
    public abstract IReadOnlyCollection<TEvent> Events { get; }

    public TState? State { get; }
    public long Version
    {
        get { lock (_acceptGate) return Events.Count; }
    }

    /// <summary>
    /// Default dependency injection constructor.
    /// </summary>
    protected CallbackEventSourcedAggregate()
        : this(new EventTransactionManager(),
                null) { }

    protected CallbackEventSourcedAggregate(
        IEventTransactionManager eventTransactionManager,
        TState? state)
    {
        _eventTransactionManager = eventTransactionManager ?? throw new ArgumentNullException(nameof(eventTransactionManager));
        State = state;
    }

    public ITransactionResult AcceptEvent(TEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        lock (_acceptGate)
        {
            if (_accepting)
            {
                var error = new InvalidOperationException("Reentrant event acceptance is not supported.");
                return new TransactionFailure(error.Message, error);
            }
            _accepting = true;
            try
            {
                if (Events.Any(existing => existing.Id == @event.Id))
                {
                    var error = new DuplicateEventException(@event.Id);
                    return new TransactionFailure(error.Message, error);
                }
                @event = _eventTransactionManager.GetEventTransactionData<TEvent>(@event, this);
                var sideEffect = PrepareSideEffect(@event);
                return _eventTransactionManager.GetTransactionScope<TEvent>(sideEffect,
                    CommitEvent, RollbackEvent).ExecuteTransaction(@event);
            }
            finally
            {
                _accepting = false;
            }
        }
    }

    protected abstract ISideEffect PrepareSideEffect(TEvent @event);

    protected abstract ITransactionResult CommitEvent(TEvent @event);

    /// <summary>Restore changes made by CommitEvent, including any event history entry.</summary>
    protected abstract ITransactionResult RollbackEvent(ref TEvent @event);
}
