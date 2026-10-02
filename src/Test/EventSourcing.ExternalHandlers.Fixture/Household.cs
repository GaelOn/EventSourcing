using EventSourcing.Handlers;
using EventSourcing.ObjectsAggregation;

namespace EventSourcing.ExternalHandlers.Fixture;

// External consumer example: unrelated event classes, complete immutable root/entity state.
public sealed record AccountData(string Name, decimal Balance) : IEntity;
public sealed record StatementData(decimal Income, decimal Expense);
public sealed record HouseholdData(AccountData Checking, AccountData Savings, StatementData Statement, int ClosedMonths)
{
    public decimal Total => Checking.Balance + Savings.Balance;
}

public sealed class IncomeReceived(Guid id, decimal amount, decimal previousBalance, decimal previousIncome,
    DateOnly date, EventMetadata? metadata = null) : EventBase(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public decimal PreviousBalance { get; } = previousBalance;
    public decimal PreviousIncome { get; } = previousIncome;
    public DateOnly Date { get; } = date;
    public override IEvent Clone(EventMetadata metadata) => new IncomeReceived(Id, Amount, PreviousBalance, PreviousIncome, Date, metadata);
}
public sealed class MoneyTransferred(Guid id, decimal amount, decimal previousChecking, decimal previousSavings,
    DateOnly date, EventMetadata? metadata = null) : EventBase(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public decimal PreviousChecking { get; } = previousChecking;
    public decimal PreviousSavings { get; } = previousSavings;
    public DateOnly Date { get; } = date;
    public override IEvent Clone(EventMetadata metadata) => new MoneyTransferred(Id, Amount, PreviousChecking, PreviousSavings, Date, metadata);
}
public sealed class ExpensePaid(Guid id, decimal amount, decimal previousBalance, decimal previousExpense,
    DateOnly date, EventMetadata? metadata = null) : EventBase(id, metadata ?? new(null, null))
{
    public decimal Amount { get; } = amount;
    public decimal PreviousBalance { get; } = previousBalance;
    public decimal PreviousExpense { get; } = previousExpense;
    public DateOnly Date { get; } = date;
    public override IEvent Clone(EventMetadata metadata) => new ExpensePaid(Id, Amount, PreviousBalance, PreviousExpense, Date, metadata);
}
public sealed class MonthClosed(Guid id, int previousCount, DateOnly date, EventMetadata? metadata = null)
    : EventBase(id, metadata ?? new(null, null))
{
    public int PreviousCount { get; } = previousCount;
    public DateOnly Date { get; } = date;
    public override IEvent Clone(EventMetadata metadata) => new MonthClosed(Id, PreviousCount, Date, metadata);
}
public sealed class IncomeCancelled(Guid id, Guid originalId, decimal amount, EventMetadata? metadata = null)
    : EventBase(id, metadata ?? new(null, null))
{
    public Guid OriginalId { get; } = originalId;
    public decimal Amount { get; } = amount;
    public override IEvent Clone(EventMetadata metadata) => new IncomeCancelled(Id, OriginalId, Amount, metadata);
}

public sealed class HouseholdHandlers : IEventHandler<HouseholdData, IncomeReceived>,
    IEventHandler<HouseholdData, MoneyTransferred>, IEventHandler<HouseholdData, ExpensePaid>,
    IEventHandler<HouseholdData, MonthClosed>, IEventHandler<HouseholdData, IncomeCancelled>
{
    private static void Require(bool valid)
    { if (!valid) throw new InvalidOperationException("Transition preconditions do not match the current household."); }
    public HouseholdData Do(HouseholdData s, IncomeReceived e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousBalance && s.Statement.Income == e.PreviousIncome);
        return s with { Checking = s.Checking with { Balance = s.Checking.Balance + e.Amount }, Statement = s.Statement with { Income = s.Statement.Income + e.Amount } };
    }
    public HouseholdData Undo(HouseholdData s, IncomeReceived e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousBalance + e.Amount && s.Statement.Income == e.PreviousIncome + e.Amount);
        return s with { Checking = s.Checking with { Balance = e.PreviousBalance }, Statement = s.Statement with { Income = e.PreviousIncome } };
    }
    public HouseholdData Do(HouseholdData s, MoneyTransferred e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousChecking && s.Savings.Balance == e.PreviousSavings && e.PreviousChecking >= e.Amount);
        return s with { Checking = s.Checking with { Balance = s.Checking.Balance - e.Amount }, Savings = s.Savings with { Balance = s.Savings.Balance + e.Amount } };
    }
    public HouseholdData Undo(HouseholdData s, MoneyTransferred e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousChecking - e.Amount && s.Savings.Balance == e.PreviousSavings + e.Amount);
        return s with { Checking = s.Checking with { Balance = e.PreviousChecking }, Savings = s.Savings with { Balance = e.PreviousSavings } };
    }
    public HouseholdData Do(HouseholdData s, ExpensePaid e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousBalance && s.Statement.Expense == e.PreviousExpense && e.PreviousBalance >= e.Amount);
        return s with { Checking = s.Checking with { Balance = s.Checking.Balance - e.Amount }, Statement = s.Statement with { Expense = s.Statement.Expense + e.Amount } };
    }
    public HouseholdData Undo(HouseholdData s, ExpensePaid e)
    {
        Require(e.Amount > 0 && s.Checking.Balance == e.PreviousBalance - e.Amount && s.Statement.Expense == e.PreviousExpense + e.Amount);
        return s with { Checking = s.Checking with { Balance = e.PreviousBalance }, Statement = s.Statement with { Expense = e.PreviousExpense } };
    }
    public HouseholdData Do(HouseholdData s, MonthClosed e)
    { Require(s.ClosedMonths == e.PreviousCount); return s with { ClosedMonths = checked(s.ClosedMonths + 1) }; }
    public HouseholdData Undo(HouseholdData s, MonthClosed e)
    { Require(s.ClosedMonths == e.PreviousCount + 1); return s with { ClosedMonths = e.PreviousCount }; }
    public HouseholdData Do(HouseholdData s, IncomeCancelled e)
    { Require(e.OriginalId != Guid.Empty && e.Amount > 0 && s.Statement.Income >= e.Amount); return s with { Checking = s.Checking with { Balance = s.Checking.Balance - e.Amount }, Statement = s.Statement with { Income = s.Statement.Income - e.Amount } }; }
    public HouseholdData Undo(HouseholdData s, IncomeCancelled e)
    {
        Require(e.OriginalId != Guid.Empty && e.Amount > 0 && s.Statement.Income >= 0);
        return s with { Checking = s.Checking with { Balance = s.Checking.Balance + e.Amount }, Statement = s.Statement with { Income = s.Statement.Income + e.Amount } };
    }
}

public sealed class HouseholdRoot : EventSourcedAggregate<HouseholdData>
{
    public HouseholdRoot(Guid id, HouseholdData opening, EventHandlerDispatcher<HouseholdData> rules) : base(id, opening, rules) { }
    public HouseholdRoot(EventSourcedAggregate<HouseholdData> restored) : base(restored) { }
    public override IEnumerable<IEntity> Entities => new[] { State.Checking, State.Savings };
    public HouseholdRoot ReceiveIncome(IncomeReceived e) => (HouseholdRoot)Apply(e);
    public static EventHandlerDispatcher<HouseholdData> Rules()
    {
        var h = new HouseholdHandlers();
        return EventHandlerDispatcher<HouseholdData>.CreateBuilder("household/v1")
            .Register<IncomeReceived>(h).Register<MoneyTransferred>(h).Register<ExpensePaid>(h)
            .Register<MonthClosed>(h).Register<IncomeCancelled>(h).Build();
    }
}
