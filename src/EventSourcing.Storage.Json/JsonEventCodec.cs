using System.Text.Json;
using System.Text.Json.Serialization;
using EventSourcing.Transactions;

namespace EventSourcing.Storage.Json;

/// <summary>Explicit stable type discriminators. No semantic identity is inferred from payloads.</summary>
public sealed class JsonEventCodec
{
    private static readonly JsonSerializerOptions Options = new() { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };
    private readonly Dictionary<string, Registration> _byName = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, string> _byType = new();
    private readonly bool _frozen;
    private sealed record Registration(Func<IEvent, JsonElement> Write, Func<JsonElement, IEvent> Read);
    /// <summary>Start a registry with the reserved journal-v1 mapping.</summary>
    public JsonEventCodec() => Register<OperationJournalEvent>("$operation-journal/v1");
    private JsonEventCodec(JsonEventCodec source)
    {
        _byName = new(source._byName, StringComparer.Ordinal); _byType = new(source._byType); _frozen = true;
    }
    /// <summary>Register one immutable event wire version before constructing a store; duplicates fail.</summary>
    public JsonEventCodec Register<TEvent>(string discriminator) where TEvent : class, IEvent
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(discriminator);
        if (_frozen) throw new InvalidOperationException("Event codec is frozen.");
        if (_byName.ContainsKey(discriminator) || _byType.ContainsKey(typeof(TEvent)))
            throw new ArgumentException("Duplicate event discriminator or event type.", nameof(discriminator));
        _byName.Add(discriminator, new(e => JsonSerializer.SerializeToElement((TEvent)e, Options),
            json => json.Deserialize<TEvent>(Options) ?? throw new InvalidDataException("Null event payload.")));
        _byType.Add(typeof(TEvent), discriminator); return this;
    }
    internal JsonEventCodec Freeze() => new(this);
    internal (string, JsonElement) Encode(IEvent e)
    {
        if (!_byType.TryGetValue(e.GetType(), out var name)) throw new InvalidDataException("Unregistered event type.");
        return (name, _byName[name].Write(e));
    }
    internal IEvent Decode(string name, JsonElement data)
    {
        if (name is null || !_byName.TryGetValue(name, out var registration))
            throw new InvalidDataException("Unsupported event wire discriminator.");
        return registration.Read(data);
    }
}
