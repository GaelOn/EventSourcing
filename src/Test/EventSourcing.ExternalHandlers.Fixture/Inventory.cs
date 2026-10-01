using EventSourcing.Handlers;

namespace EventSourcing.ExternalHandlers.Fixture;

public sealed record Item(string Name, int Quantity);
public sealed record Inventory(Item OwnedItem);
public sealed record ReceiptConfiguration(int Multiplier);
public abstract class InventoryEvent(Guid id, EventMetadata? metadata = null)
    : EventBase<InventoryEvent>(id, metadata ?? new(null, null));
public sealed class Received(Guid id, int quantity, EventMetadata? metadata = null) : InventoryEvent(id, metadata)
{
    public int Quantity { get; } = quantity;
    public override InventoryEvent Clone(EventMetadata metadata) => new Received(Id, Quantity, metadata);
}
public sealed class Renamed(Guid id, string previous, string next, EventMetadata? metadata = null) : InventoryEvent(id, metadata)
{
    public string Previous { get; } = previous;
    public string Next { get; } = next;
    public override InventoryEvent Clone(EventMetadata metadata) => new Renamed(Id, Previous, Next, metadata);
}
public sealed class Unregistered(Guid id, EventMetadata? metadata = null) : InventoryEvent(id, metadata)
{
    public override InventoryEvent Clone(EventMetadata metadata) => new Unregistered(Id, metadata);
}
public sealed class ReceiptHandler(ReceiptConfiguration configuration) : IEventHandler<Inventory, Received>
{
    public ReceiptConfiguration Configuration { get; } = configuration;
    public Inventory Do(Inventory state, Received e) => state with
    { OwnedItem = state.OwnedItem with { Quantity = checked(state.OwnedItem.Quantity + e.Quantity * Configuration.Multiplier) } };
    public Inventory Undo(Inventory state, Received e) => state with
    { OwnedItem = state.OwnedItem with { Quantity = checked(state.OwnedItem.Quantity - e.Quantity * Configuration.Multiplier) } };
}
public sealed class RenameHandler : IEventHandler<Inventory, Renamed>
{
    public Inventory Do(Inventory state, Renamed e) => state.OwnedItem.Name == e.Previous
        ? state with { OwnedItem = state.OwnedItem with { Name = e.Next } } : throw new InvalidOperationException("Previous name mismatch.");
    public Inventory Undo(Inventory state, Renamed e) => state.OwnedItem.Name == e.Next
        ? state with { OwnedItem = state.OwnedItem with { Name = e.Previous } } : throw new InvalidOperationException("Immediate resulting name required.");
}
public sealed class DoubleReceiptHandler : IEventHandler<Inventory, Received>
{
    public Inventory Do(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity + 2 * e.Quantity } };
    public Inventory Undo(Inventory s, Received e) => s with { OwnedItem = s.OwnedItem with { Quantity = s.OwnedItem.Quantity - 2 * e.Quantity } };
}
