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
    /// <summary>Coordinator for the legacy reversible callback lifecycle.</summary>
    /// <summary>Coordinator for the legacy synchronous callback lifecycle.</summary>
    protected readonly IEventTransactionManager _eventTransactionManager;

    /// <inheritdoc />
    /// <summary>Caller-supplied aggregate identity.</summary>
    public abstract Guid Id { get; }
    /// <inheritdoc />
    /// <summary>Entities owned by the business root.</summary>
    public abstract IEnumerable<IEntity> Entities { get; }
    /// <summary>Accepted events owned by the legacy aggregate.</summary>
    /// <summary>Complete committed history; the callback version is count-based.</summary>
    public abstract IReadOnlyCollection<TEvent> Events { get; }

    /// <summary>Optional application-provided state for the callback lifecycle.</summary>
    /// <summary>Optional business state supplied by the callback aggregate.</summary>
    public TState? State { get; }
    /// <summary>Current position derived from accepted event count.</summary>
    /// <summary>Position derived from the complete event history count.</summary>
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

    /// <summary>Construct a callback aggregate with an explicit coordinator and optional state.</summary>
    /// <summary>Supply a coordinator and optional business state.</summary>
    protected CallbackEventSourcedAggregate(
        IEventTransactionManager eventTransactionManager,
        TState? state)
    {
        _eventTransactionManager = eventTransactionManager ?? throw new ArgumentNullException(nameof(eventTransactionManager));
        State = state;
    }

    /// <summary>Validate and accept one event through the synchronous reversible callback lifecycle.</summary>
    /// <summary>Serialize acceptance, assign metadata, prepare, commit and apply with reversible recovery.</summary>
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
                    var error = new DuplicateEventException(@event.Id, Id);
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

    /// <summary>Prepare the reversible effect without applying it.</summary>
    /// <summary>Prepare a reversible effect without observable changes.</summary>
    protected abstract ISideEffect PrepareSideEffect(TEvent @event);

    /// <summary>Commit resources owned by this aggregate; rollback must restore partially completed commits.</summary>
    /// <summary>Commit owned resources; explicit rejection must leave them unchanged.</summary>
    protected abstract ITransactionResult CommitEvent(TEvent @event);

    /// <summary>Restore changes made by CommitEvent, including any event history entry.</summary>
    protected abstract ITransactionResult RollbackEvent(ref TEvent @event);
}
