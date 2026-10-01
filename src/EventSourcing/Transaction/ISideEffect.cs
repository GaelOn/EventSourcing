namespace EventSourcing.Transaction;

public interface ISideEffect
{
    IApplyStatus ApplySideEffect();
    IApplyStatus Undo();
}

public interface IApplyStatus { }

public sealed record ApplySuccess : IApplyStatus {}

public sealed record ApplyFailure(string Reason, Exception Exception) : IApplyStatus {}
