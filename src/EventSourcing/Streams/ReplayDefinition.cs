namespace EventSourcing.Streams;

/// <summary>
/// Opening inputs and the identity of the rules used to interpret a stream.
/// Applications must persist this alongside the history and retain the matching reducer.
/// Opening state must be immutable, including any referenced objects.
/// </summary>
public sealed record ReplayDefinition<TState> where TState : class
{
    /// <summary>Immutable original inputs from which full replay begins.</summary>
    public TState OpeningState { get; }
    /// <summary>Application-owned identity of the calculation rules and immutable configuration.</summary>
    public string RuleVersion { get; }

    /// <summary>Bind immutable opening inputs to the application-owned calculation rule identity.</summary>
    public ReplayDefinition(TState openingState, string ruleVersion)
    {
        ArgumentNullException.ThrowIfNull(openingState);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        OpeningState = openingState;
        RuleVersion = ruleVersion;
    }
}

/// <summary>Recorded calculation rules differ from the supplied calculation identity.</summary>
public sealed class ReplayRuleVersionException : InvalidOperationException
{
    /// <summary>The recorded rule identity.</summary>
    public string RecordedVersion { get; }
    /// <summary>The supplied calculation identity.</summary>
    public string ReducerVersion { get; }
    /// <summary>The affected stream when known.</summary>
    public Guid? StreamId { get; }
    /// <summary>Create a rule compatibility diagnostic.</summary>
    public ReplayRuleVersionException(string recordedVersion, string reducerVersion, Guid? streamId = null)
        : base($"History requires rules '{recordedVersion}', but the reducer identifies rules '{reducerVersion}'.")
    { RecordedVersion = recordedVersion; ReducerVersion = reducerVersion; StreamId = streamId; }
}
