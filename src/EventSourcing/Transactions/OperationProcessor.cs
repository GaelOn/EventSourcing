namespace EventSourcing.Transactions;

/// <summary>Admission wraps isolated event processing; the injected coordinator owns all publication guarantees.</summary>
public sealed class OperationProcessor(IEventTransactionCoordinator coordinator)
{
    private readonly IEventTransactionCoordinator _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
    /// <summary>
    /// Run consequences only for an owned claim. The callback must create isolated immutable branches,
    /// never mutate shared state or perform external effects. Wait timeout returns InProgress, not permission to execute.
    /// </summary>
    public async Task<OperationResult> ExecuteAsync(OperationKey key, string contractVersion,
        Func<CancellationToken, Task<OperationPlan>> process, TimeSpan wait = default,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(process);
        if (wait < TimeSpan.Zero || wait > TimeSpan.FromMinutes(5)) throw new ArgumentOutOfRangeException(nameof(wait));
        var admission = await _coordinator.BeginAsync(key, contractVersion, cancellationToken).ConfigureAwait(false);
        if (admission.Status == OperationStatus.InProgress)
        {
            var resolved = await _coordinator.ResolveAsync(key, contractVersion, wait, cancellationToken).ConfigureAwait(false);
            if (resolved.Status != OperationStatus.RetryAllowed) return resolved;
            admission = await _coordinator.BeginAsync(key, contractVersion, cancellationToken).ConfigureAwait(false);
        }
        if (admission.Status != OperationStatus.Claimed) return admission;
        var handle = admission.Handle ?? throw new OperationIntegrityException("Claim result has no ownership capability.");
        try
        {
            var plan = await process(cancellationToken).ConfigureAwait(false);
            await _coordinator.StageAsync(handle, plan, cancellationToken).ConfigureAwait(false);
            return await _coordinator.CommitAsync(handle, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception processingFailure)
        {
            try { await _coordinator.RollbackAsync(handle, CancellationToken.None).ConfigureAwait(false); }
            catch (Exception rollbackFailure)
            { throw new AggregateException("Processing failed and rollback is unresolved; recover before retrying.", processingFailure, rollbackFailure); }
            throw;
        }
    }
}
