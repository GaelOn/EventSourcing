namespace EventSourcing.Streams;

/// <summary>
/// Atomic append for one stream. Implementations must serialize expected-version checks and writes.
/// This port stores events only, not opening inputs or rule versions. Applications must durably bind
/// their ReplayDefinition to the stream and reload it with history before replaying a scenario.
/// </summary>
public interface IEventStreamStore<TEvent> where TEvent : class, IEvent<TEvent>
{
    Task<IReadOnlyList<TEvent>> ReadAsync(Guid streamId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reject stale versions, duplicate IDs across stored and new events, and invalid stream metadata.
    /// Failure must preserve storage. Successful return means the complete batch is committed.
    /// </summary>
    Task AppendAsync(Guid streamId, long expectedVersion, IReadOnlyList<TEvent> events,
        CancellationToken cancellationToken = default);
}
