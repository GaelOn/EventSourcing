using EventSourcing.Transaction;

namespace EventSourcing.Streams;

public sealed class DuplicateEventException(Guid id)
    : InvalidOperationException($"Event '{id}' is already present in this stream.");

public sealed class StreamConcurrencyException(long expected, long actual)
    : InvalidOperationException($"Expected stream version {expected}, but storage is at version {actual}.");

/// <summary>Shared validation for replay and persistence adapters. Order is stream order, not effective date.</summary>
public static class EventHistory
{
    public static void Validate<TEvent>(Guid streamId, IEnumerable<TEvent> events)
        where TEvent : class, IEvent<TEvent>
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        ArgumentNullException.ThrowIfNull(events);
        var ids = new HashSet<Guid>();
        long version = 0;
        foreach (var @event in events)
        {
            ArgumentNullException.ThrowIfNull(@event);
            if (@event.Id == Guid.Empty) throw new InvalidOperationException("An event ID is required.");
            if (!ids.Add(@event.Id)) throw new DuplicateEventException(@event.Id);
            if (@event.TransactionnalData != new TransactionnalData(checked(++version), streamId))
                throw new InvalidOperationException("Event stream identity or version is inconsistent.");
        }
    }

    internal static TEvent Tag<TEvent>(TEvent @event, Guid streamId, long version)
        where TEvent : class, IEvent<TEvent>
    {
        if (@event.Id == Guid.Empty) throw new ArgumentException("An event ID is required.", nameof(@event));
        if (@event.TransactionnalData != new TransactionnalData(null, null))
            throw new InvalidOperationException("Recorded events must be replayed, not accepted again.");
        var metadata = new TransactionnalData(version, streamId);
        var tagged = @event.Clone(metadata);
        if (tagged is null || tagged.Id != @event.Id || tagged.TransactionnalData != metadata)
            throw new InvalidOperationException("Cloning must preserve event identity and assign the requested metadata.");
        return tagged;
    }
}
