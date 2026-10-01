using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class CorePackageTests
{
    [Theory]
    [InlineData("EventSourcing.ObjectsAggregation.EventSourcedAggregate`2")]
    [InlineData("EventSourcing.ObjectsAggregation.IAggregate")]
    [InlineData("EventSourcing.Transaction.IEventTransactionManager")]
    public void Core_consumers_do_not_receive_the_legacy_callback_API(string typeName)
    {
        Assert.Null(typeof(EventStream<,>).Assembly.GetType(typeName));
    }
}
