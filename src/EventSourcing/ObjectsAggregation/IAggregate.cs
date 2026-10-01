namespace EventSourcing.ObjectsAggregation;

/// <summary>A business root with a stable identity, stream position and projected entities.</summary>
public interface IAggregate
{
    /// <summary>Caller-supplied identity, independent of stream position.</summary>
    Guid Id { get; }
    /// <summary>Current stream position, including pending events.</summary>
    long Version { get; }
    /// <summary>Business entities projected from the complete aggregate state.</summary>
    IEnumerable<IEntity> Entities { get; }
}
