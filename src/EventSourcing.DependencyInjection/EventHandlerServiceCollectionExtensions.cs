using EventSourcing.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing.DependencyInjection;

/// <summary>Optional startup composition and explicit rule-version selection for core dispatchers.</summary>
public static class EventHandlerServiceCollectionExtensions
{
    /// <summary>
    /// Register one identified dispatcher and its constructor-injected handlers. Registrations are frozen
    /// before publication; duplicate rule sets fail rather than replacing an existing keyed dispatcher.
    /// Scoped is the default lifetime. Enable container scope validation to detect captive dependencies.
    /// </summary>
    public static IServiceCollection AddEventHandlerDispatcher<TState, TEvent>(this IServiceCollection services,
        string ruleVersion, Action<EventHandlerRegistrationBuilder<TState, TEvent>> configure,
        ServiceLifetime dispatcherLifetime = ServiceLifetime.Scoped)
        where TState : class where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        ArgumentNullException.ThrowIfNull(configure);
        ValidateLifetime(dispatcherLifetime);
        var dispatcherType = typeof(EventHandlerDispatcher<TState, TEvent>);
        if (services.Any(d => d.ServiceType == dispatcherType && d.IsKeyedService && Equals(d.ServiceKey, ruleVersion)))
            throw new ArgumentException($"Dispatcher rules '{ruleVersion}' are already registered for '{typeof(TState)}' and '{typeof(TEvent)}'.", nameof(ruleVersion));
        var builder = new EventHandlerRegistrationBuilder<TState, TEvent>();
        configure(builder);
        var handlers = builder.Freeze();
        var key = new HandlerRegistrationKey(typeof(TState), typeof(TEvent), ruleVersion);
        var descriptors = handlers.GroupBy(h => h.HandlerType).Select(g =>
            ServiceDescriptor.DescribeKeyed(g.Key, key, g.Key, g.First().Lifetime)).ToArray();
        services.Add(ServiceDescriptor.DescribeKeyed(typeof(DispatcherLease), key, (_, _) => new DispatcherLease(), dispatcherLifetime));
        foreach (var descriptor in descriptors) services.Add(descriptor);
        services.Add(ServiceDescriptor.DescribeKeyed(dispatcherType, ruleVersion, (provider, _) =>
        {
            var lease = provider.GetRequiredKeyedService<DispatcherLease>(key);
            var dispatcher = EventHandlerDispatcher<TState, TEvent>.CreateBuilder(ruleVersion);
            var retained = new Dictionary<Type, object>();
            foreach (var handler in handlers) handler.Register(dispatcher, provider, key, lease, retained);
            return dispatcher.Build();
        }, dispatcherLifetime));
        return services;
    }

    /// <summary>Resolve the requested rule version from the current scope; never select or replace a default silently.</summary>
    public static EventHandlerDispatcher<TState, TEvent> GetEventHandlerDispatcher<TState, TEvent>(
        this IServiceProvider provider, string ruleVersion)
        where TState : class where TEvent : class, IEvent<TEvent>
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleVersion);
        return provider.GetRequiredKeyedService<EventHandlerDispatcher<TState, TEvent>>(ruleVersion);
    }

    internal static void ValidateLifetime(ServiceLifetime lifetime)
    {
        if (!Enum.IsDefined(lifetime)) throw new ArgumentOutOfRangeException(nameof(lifetime));
    }
    private sealed record HandlerRegistrationKey(Type State, Type StreamEvent, string RuleVersion);
}

/// <summary>
/// Fluent, single-threaded startup builder. Handlers and their configuration must remain pure and immutable.
/// A handler implementing multiple event interfaces is retained once per dispatcher construction.
/// </summary>
public sealed class EventHandlerRegistrationBuilder<TState, TEvent>
    where TState : class where TEvent : class, IEvent<TEvent>
{
    private readonly List<IHandlerRegistration<TState, TEvent>> _handlers = [];
    private bool _frozen;
    internal EventHandlerRegistrationBuilder() { }

    /// <summary>
    /// Map one concrete event to a constructor-injected handler with an explicit lifetime. Reusing a handler
    /// type for several event interfaces requires the same lifetime and retains the same instance.
    /// </summary>
    public EventHandlerRegistrationBuilder<TState, TEvent> AddHandler<THandledEvent, THandler>(
        ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where THandledEvent : class, TEvent where THandler : class, IEventHandler<TState, THandledEvent>
    {
        if (_frozen) throw new InvalidOperationException("Dispatcher registrations are frozen.");
        EventHandlerServiceCollectionExtensions.ValidateLifetime(lifetime);
        if (typeof(THandler).IsAbstract || typeof(THandler).IsInterface)
            throw new ArgumentException("Handler implementations must be concrete.", nameof(THandler));
        if (_handlers.Any(h => h.EventType == typeof(THandledEvent)))
            throw new ArgumentException($"A handler is already registered for '{typeof(THandledEvent)}'.", nameof(THandledEvent));
        if (_handlers.Any(h => h.HandlerType == typeof(THandler) && h.Lifetime != lifetime))
            throw new ArgumentException("All mappings for one handler implementation must use the same lifetime.", nameof(lifetime));
        _handlers.Add(new HandlerRegistration<TState, TEvent, THandledEvent, THandler>(lifetime));
        return this;
    }

    internal IHandlerRegistration<TState, TEvent>[] Freeze()
    { _frozen = true; return _handlers.ToArray(); }
}

internal interface IHandlerRegistration<TState, TEvent>
    where TState : class where TEvent : class, IEvent<TEvent>
{
    Type EventType { get; }
    Type HandlerType { get; }
    ServiceLifetime Lifetime { get; }
    void Register(EventHandlerDispatcherBuilder<TState, TEvent> dispatcher, IServiceProvider provider,
        object key, DispatcherLease lease, Dictionary<Type, object> retained);
}

internal sealed class HandlerRegistration<TState, TEvent, THandledEvent, THandler>(ServiceLifetime lifetime)
    : IHandlerRegistration<TState, TEvent>
    where TState : class where TEvent : class, IEvent<TEvent>
    where THandledEvent : class, TEvent where THandler : class, IEventHandler<TState, THandledEvent>
{
    public Type EventType => typeof(THandledEvent);
    public Type HandlerType => typeof(THandler);
    public ServiceLifetime Lifetime => lifetime;
    public void Register(EventHandlerDispatcherBuilder<TState, TEvent> dispatcher, IServiceProvider provider,
        object key, DispatcherLease lease, Dictionary<Type, object> retained)
    {
        if (!retained.TryGetValue(typeof(THandler), out var instance))
        {
            instance = provider.GetRequiredKeyedService<THandler>(key);
            retained.Add(typeof(THandler), instance);
        }
        dispatcher.Register<THandledEvent>(new RetainedHandler<TState, THandledEvent>((THandler)instance, lease));
    }
}

internal sealed class RetainedHandler<TState, TEvent>(IEventHandler<TState, TEvent> handler, DispatcherLease lease)
    : IEventHandler<TState, TEvent> where TState : class where TEvent : class
{
    public TState Do(TState state, TEvent e) { lease.EnsureActive(); return handler.Do(state, e); }
    public TState Undo(TState state, TEvent e) { lease.EnsureActive(); return handler.Undo(state, e); }
}

internal sealed class DispatcherLease : IDisposable
{
    private int _disposed;
    public void EnsureActive() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    public void Dispose() => Interlocked.Exchange(ref _disposed, 1);
}
