namespace EventSourcing.Streams;

/// <summary>
/// Opening inputs and the identity of the rules used to interpret a stream.
/// Applications must persist this alongside the history and retain the matching reducer.
/// Opening state must be immutable, including any referenced objects.
/// </summary>
public sealed record ReplayDefinition<TState> where TState : class
{
    public TState OpeningState { get; }
    public string RuleVersion { get; }

    public ReplayDefinition(TState openingState, string ruleVersion)
    {
        ArgumentNullException.ThrowIfNull(openingState);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        OpeningState = openingState;
        RuleVersion = ruleVersion;
    }
}

public sealed class ReplayRuleVersionException(string recordedVersion, string reducerVersion)
    : InvalidOperationException($"History requires rules '{recordedVersion}', but the reducer identifies rules '{reducerVersion}'.")
{
    public string RecordedVersion { get; } = recordedVersion;
    public string ReducerVersion { get; } = reducerVersion;
}
