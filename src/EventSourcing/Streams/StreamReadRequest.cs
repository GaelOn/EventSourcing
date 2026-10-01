namespace EventSourcing.Streams;

/// <summary>Read a committed stream suffix after a version, exclusively.</summary>
public sealed record StreamReadRequest
{
    public Guid StreamId { get; }
    public long AfterVersion { get; }

    public StreamReadRequest(Guid streamId, long afterVersion)
    {
        if (streamId == Guid.Empty) throw new ArgumentException("A stream ID is required.", nameof(streamId));
        if (afterVersion < 0) throw new ArgumentOutOfRangeException(nameof(afterVersion));
        StreamId = streamId;
        AfterVersion = afterVersion;
    }
}
