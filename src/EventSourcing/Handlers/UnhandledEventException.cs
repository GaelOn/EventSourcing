namespace EventSourcing.Handlers;

/// <summary>No exact handler exists for an event's runtime type.</summary>
public sealed class UnhandledEventException : InvalidOperationException
{
    public Type EventType { get; }

    public UnhandledEventException(Type eventType)
        : base($"No handler is registered for event type '{(eventType ?? throw new ArgumentNullException(nameof(eventType))).FullName}'.") =>
        EventType = eventType;
}
