namespace EventSourcing.Transaction;

/// <summary>Result of the legacy synchronous reversible callback lifecycle.</summary>
public interface ITransactionResult {}

/// <summary>The commit and subsequent application succeeded.</summary>
public sealed record TransactionSuccess : ITransactionResult;

/// <summary>The commit's owned resources were restored.</summary>
public sealed record TransactionRollbacked : ITransactionResult;

/// <summary>A rejection or recovery failure, retaining its explanation and cause.</summary>
/// <param name="Reason">Human-readable failure explanation.</param>
/// <param name="Exception">Original error or aggregate recovery errors.</param>
public sealed record TransactionFailure(string Reason, Exception Exception) : ITransactionResult;
