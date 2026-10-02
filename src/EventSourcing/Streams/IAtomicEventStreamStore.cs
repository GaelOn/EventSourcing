namespace EventSourcing.Streams;

/// <summary>
/// Thread-safe storage within one transaction boundary. Every batch is all-or-nothing;
/// adapters must not implement this by looping over independent single-stream commits.
/// </summary>
public interface IAtomicEventStreamStore : IEventStreamStore
{
    /// <summary>Read all requested streams from one consistent storage snapshot.</summary>
    Task<IReadOnlyDictionary<Guid, IReadOnlyList<IEvent>>> ReadBatchAsync(IReadOnlyList<Guid> streamIds,
        CancellationToken cancellationToken = default);

    /// <summary>Read suffixes, prefix identity indexes, and versions for all participants from one view.</summary>
    Task<IReadOnlyDictionary<Guid, StreamSlice>> ReadBatchFromAsync(
        IReadOnlyList<StreamReadRequest> requests, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically check every expected version, validate every append, and publish the complete batch.
    /// Empty participant appends still assert their expected versions. Duplicate stream IDs are invalid.
    /// Cancellation before commit preserves all streams; after commit the operation must report success.
    /// </summary>
    Task AppendBatchAsync(IReadOnlyList<StreamAppend> appends,
        CancellationToken cancellationToken = default);
}
