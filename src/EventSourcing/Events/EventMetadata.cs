namespace EventSourcing;

/// <summary>Unassigned events have null values; recorded events have a one-based stream version.</summary>
public sealed record EventMetadata(long? Version, Guid? StreamId);
