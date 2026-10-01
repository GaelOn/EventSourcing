namespace EventSourcing.Transaction;

/// <summary>Unassigned events have null values; recorded events have a one-based stream version.</summary>
public sealed record TransactionnalData(long? Version, Guid? AggregateId);
