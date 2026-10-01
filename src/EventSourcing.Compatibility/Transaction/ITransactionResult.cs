namespace EventSourcing.Transaction;

public interface ITransactionResult {}

public sealed record TransactionSuccess : ITransactionResult;

public sealed record TransactionRollbacked : ITransactionResult;

public sealed record TransactionFailure(string Reason, Exception Exception) : ITransactionResult;
