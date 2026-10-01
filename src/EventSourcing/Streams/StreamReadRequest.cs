namespace EventSourcing.Streams;

/// <summary>Read a committed stream suffix after a version, exclusively.</summary>
public sealed record StreamReadRequest
{
    /// <summary>Identity of the stream participating in this operation.</summary>
    public Guid StreamId { get; }
    /// <summary>Exclusive cursor preceding the requested suffix.</summary>
    public long AfterVersion { get; }

    /// <summary>Create a validated exclusive suffix cursor.</summary>
    public StreamReadRequest(Guid streamId, long afterVersion)
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        if (afterVersion < 0) throw new ArgumentOutOfRangeException(nameof(afterVersion));
        StreamId = streamId;
        AfterVersion = afterVersion;
    }
}
