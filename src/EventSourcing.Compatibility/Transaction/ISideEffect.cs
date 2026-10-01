namespace EventSourcing.Transaction;

/// <summary>A legacy reversible synchronous effect; implementations must restore partially applied changes.</summary>
public interface ISideEffect
{
    /// <summary>Apply the prepared effect after a successful commit.</summary>
    IApplyStatus ApplySideEffect();
    /// <summary>Restore resources after failed or partially completed application.</summary>
    IApplyStatus Undo();
}

/// <summary>Result of applying or reversing a legacy side effect.</summary>
public interface IApplyStatus { }

/// <summary>The side-effect operation completed successfully.</summary>
public sealed record ApplySuccess : IApplyStatus {}

/// <summary>The side-effect operation failed; the coordinator may attempt recovery.</summary>
/// <param name="Reason">Failure explanation.</param>
/// <param name="Exception">Underlying failure cause.</param>
public sealed record ApplyFailure(string Reason, Exception Exception) : IApplyStatus {}
