using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;

/// <summary>Optional representative baseline; the default command retains the complete parameter matrix.</summary>
public static class BaselineConfig
{
    public static IConfig Create()
    {
        var config = ManualConfig.Create(DefaultConfig.Instance);
        if (Environment.GetEnvironmentVariable("EVENTSOURCING_BASELINE") == "1")
            config.AddFilter(new RepresentativeCases());
        return config;
    }

    private sealed class RepresentativeCases : IFilter
    {
        public bool Predicate(BenchmarkCase benchmarkCase)
        {
            var values = benchmarkCase.Parameters.Items.ToDictionary(p => p.Name, p => (int)p.Value);
            return values["SuffixLength"] == 1 && values["ParticipantCount"] == 2;
        }
    }
}
