namespace EventSourcing.ObjectsAggregation;

public interface IAggregate
{
    Guid Id { get; }
    long Version { get; }
    IEnumerable<IEntity> Entities { get; }
}
