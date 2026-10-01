using EventSourcing.Transaction;

namespace EventSourcing.Tests;

public sealed class TransactionScopeTests
{
    [Fact]
    public void Successful_transaction_commits_before_applying_and_does_not_roll_back()
    {
        var order = new List<string>();
        var effect = new TestSideEffect { Apply = () => { order.Add("apply"); return new ApplySuccess(); } };
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect,
            e => { order.Add("commit"); return new TransactionSuccess(); },
            (ref TestEvent e) => { order.Add("rollback"); return new TransactionRollbacked(); });

        Assert.IsType<TransactionSuccess>(scope.ExecuteTransaction(TestEvent.Create()));
        Assert.Equal(new[] { "commit", "apply" }, order);
        Assert.Equal(0, effect.UndoCalls);
    }

    [Fact]
    public void Reported_application_failure_invokes_rollback_and_retains_its_cause()
    {
        var exception = new InvalidOperationException("Application failed");
        var effect = new TestSideEffect { Apply = () => new ApplyFailure("Application failed", exception) };
        var rollbackCalls = 0;
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e => new TransactionSuccess(),
            (ref TestEvent e) => { rollbackCalls++; return new TransactionRollbacked(); });

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));

        Assert.Equal(1, rollbackCalls);
        Assert.Same(exception, failure.Exception);
        Assert.Contains("Application failed", failure.Reason);
    }

    [Fact]
    public void Rollback_failure_is_visible_to_the_caller()
    {
        var effect = new TestSideEffect
        {
            Apply = () => new ApplyFailure("Application failed", new InvalidOperationException())
        };
        var rollbackFailure = new TransactionFailure("Rollback failed", new IOException());
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e => new TransactionSuccess(),
            (ref TestEvent e) => rollbackFailure);

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));
        Assert.Contains(rollbackFailure.Exception,
            Assert.IsType<AggregateException>(failure.Exception).InnerExceptions);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Partial_application_is_undone_before_commit_is_rolled_back(bool throws)
    {
        var balance = 100;
        var committed = false;
        var cause = new InvalidOperationException("Application failed");
        var order = new List<string>();
        var effect = new TestSideEffect
        {
            Apply = () =>
            {
                balance -= 25;
                if (throws) throw cause;
                return new ApplyFailure("Application failed", cause);
            },
            Revert = () => { order.Add("undo"); balance = 100; return new ApplySuccess(); }
        };
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect,
            e => { committed = true; return new TransactionSuccess(); },
            (ref TestEvent e) => { order.Add("rollback"); committed = false; return new TransactionRollbacked(); });

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));

        Assert.Same(cause, failure.Exception);
        Assert.Equal(100, balance);
        Assert.False(committed);
        Assert.Equal(new[] { "undo", "rollback" }, order);
    }

    [Fact]
    public void Rejected_commit_preserves_failure_without_applying_or_undoing()
    {
        var rejected = new TransactionFailure("Rejected", new IOException());
        var effect = new TestSideEffect();
        var rollbackCalls = 0;
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e => rejected,
            (ref TestEvent e) => { rollbackCalls++; return new TransactionRollbacked(); });

        Assert.Same(rejected, scope.ExecuteTransaction(TestEvent.Create()));
        Assert.Equal(0, effect.ApplyCalls);
        Assert.Equal(0, effect.UndoCalls);
        Assert.Equal(0, rollbackCalls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Failed_undo_still_attempts_rollback_and_preserves_all_causes(bool throws)
    {
        var application = new InvalidOperationException("Application failed");
        var undo = new IOException("Undo failed");
        var rollback = new IOException("Rollback failed");
        var effect = new TestSideEffect
        {
            Apply = () => new ApplyFailure("Application failed", application),
            Revert = () => throws ? throw undo : new ApplyFailure("Undo failed", undo)
        };
        var rollbackCalls = 0;
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e => new TransactionSuccess(),
            (ref TestEvent e) =>
            {
                rollbackCalls++;
                if (throws) throw rollback;
                return new TransactionFailure("Rollback failed", rollback);
            });

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));

        Assert.Equal(1, rollbackCalls);
        Assert.Equal(new Exception[] { application, undo, rollback },
            Assert.IsType<AggregateException>(failure.Exception).InnerExceptions);
    }

    [Fact]
    public void Thrown_commit_attempts_rollback_without_touching_side_effect()
    {
        var cause = new IOException("Commit interrupted");
        var effect = new TestSideEffect();
        var rollbackCalls = 0;
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e => throw cause,
            (ref TestEvent e) => { rollbackCalls++; return new TransactionRollbacked(); });

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));

        Assert.Same(cause, failure.Exception);
        Assert.Equal(1, rollbackCalls);
        Assert.Equal(0, effect.ApplyCalls);
        Assert.Equal(0, effect.UndoCalls);
    }
    [Theory]
    [InlineData("commit", false)]
    [InlineData("commit", true)]
    [InlineData("apply", false)]
    [InlineData("apply", true)]
    [InlineData("undo", false)]
    [InlineData("undo", true)]
    [InlineData("rollback", false)]
    [InlineData("rollback", true)]
    public void Invalid_callback_results_cannot_report_success_and_do_not_prevent_recovery(
        string stage, bool returnsNull)
    {
        IApplyStatus invalidApply = returnsNull ? null! : new UnknownApplyStatus();
        ITransactionResult invalidTransaction = returnsNull ? null! : new UnknownTransactionResult();
        var effect = new TestSideEffect
        {
            Apply = () => stage == "apply" ? invalidApply : new ApplyFailure("Failed", new IOException()),
            Revert = () => stage == "undo" ? invalidApply : new ApplySuccess()
        };
        var rollbackCalls = 0;
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect,
            e => stage == "commit" ? invalidTransaction : new TransactionSuccess(),
            (ref TestEvent e) =>
            {
                rollbackCalls++;
                return stage == "rollback" ? invalidTransaction : new TransactionRollbacked();
            });

        var failure = Assert.IsType<TransactionFailure>(scope.ExecuteTransaction(TestEvent.Create()));

        Assert.Equal(1, rollbackCalls);
        Assert.Equal(stage == "commit" ? 0 : 1, effect.UndoCalls);
        if (stage is "undo" or "rollback")
        {
            Assert.Contains("Recovery incomplete", failure.Reason);
            Assert.IsType<AggregateException>(failure.Exception);
        }
        else
            Assert.IsType<InvalidOperationException>(failure.Exception);
    }

    [Fact]
    public async Task Shared_transaction_scope_does_not_interleave_callbacks()
    {
        var active = 0;
        var overlapped = 0;
        var effect = new TestSideEffect { Apply = () =>
        {
            Interlocked.Decrement(ref active);
            return new ApplySuccess();
        } };
        IEventTransactionManager manager = new EventTransactionManager();
        var scope = manager.GetTransactionScope<TestEvent>(effect, e =>
        {
            if (Interlocked.Increment(ref active) > 1) Interlocked.Exchange(ref overlapped, 1);
            Thread.SpinWait(10000);
            return new TransactionSuccess();
        }, (ref TestEvent e) => new TransactionRollbacked());
        var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ =>
            Task.Run(() => scope.ExecuteTransaction(TestEvent.Create()))));
        Assert.All(results, result => Assert.IsType<TransactionSuccess>(result));
        Assert.Equal(0, active);
        Assert.Equal(0, overlapped);
    }

    [Fact]
    public void Shared_transaction_scope_rejects_reentrant_execution()
    {
        ITransactionScope<TestEvent>? scope = null;
        ITransactionResult? nested = null;
        var attempted = false;
        IEventTransactionManager manager = new EventTransactionManager();
        scope = manager.GetTransactionScope<TestEvent>(new TestSideEffect(), e =>
        {
            if (!attempted)
            {
                attempted = true;
                nested = scope!.ExecuteTransaction(e);
            }
            return new TransactionSuccess();
        }, (ref TestEvent e) => new TransactionRollbacked());
        Assert.IsType<TransactionSuccess>(scope.ExecuteTransaction(TestEvent.Create()));
        Assert.IsType<TransactionFailure>(nested);
    }

    private sealed record UnknownApplyStatus : IApplyStatus;
    private sealed record UnknownTransactionResult : ITransactionResult;
}
