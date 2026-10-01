using EventSourcing.Streams;

namespace EventSourcing.Tests;

public sealed class CorePackageTests
{
    [Theory]
    [InlineData("EventSourcing.ObjectsAggregation.CallbackEventSourcedAggregate`2")]
    [InlineData("EventSourcing.Transaction.ISideEffect")]
    [InlineData("EventSourcing.Transaction.IEventTransactionManager")]
    public void Core_consumers_do_not_receive_the_legacy_callback_API(string typeName)
    {
        Assert.Null(typeof(EventStream<,>).Assembly.GetType(typeName));
    }
}
