namespace EventSourcing.Handlers;

/// <summary>No exact handler exists for an event's runtime type.</summary>
public sealed class UnhandledEventException : InvalidOperationException
{
    /// <summary>Concrete runtime type for which no handler was registered.</summary>
    public Type EventType { get; }

    /// <summary>Identify an unregistered concrete event type.</summary>
    public UnhandledEventException(Type eventType)
        : base($"No handler is registered for event type '{(eventType ?? throw new ArgumentNullException(nameof(eventType))).FullName}'.") =>
        EventType = eventType;
}
