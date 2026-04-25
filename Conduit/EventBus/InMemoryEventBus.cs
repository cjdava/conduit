using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.Handlers;

namespace Conduit.EventBus;

/// <summary>
/// In-process event bus. Acts as the Observable subject — it maintains a registry of
/// handlers (observers) per event type and fans out published events to all subscribers.
///
/// Thread-safety note: subscriptions are mutated only at startup; concurrent publishes are safe.
/// </summary>
public sealed class InMemoryEventBus : IEventBus
{
    private readonly Dictionary<Type, List<object>> _handlers = new();

    public void Subscribe<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEtlEvent
    {
        var eventType = typeof(TEvent);
        if (!_handlers.TryGetValue(eventType, out var list))
        {
            list = new List<object>();
            _handlers[eventType] = list;
        }

        list.Add(handler);
    }

    public void Unsubscribe<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEtlEvent
    {
        if (_handlers.TryGetValue(typeof(TEvent), out var list))
            list.Remove(handler);
    }

    // ── Callback-based subscriptions ──────────────────────────────────────────
    public DelegateEventHandler<TEvent> Subscribe<TEvent>(Func<TEvent, CancellationToken, Task> callback)
        where TEvent : IEtlEvent
    {
        var handler = new DelegateEventHandler<TEvent>(callback);
        Subscribe(handler);
        return handler;
    }

    public DelegateEventHandler<TEvent> Subscribe<TEvent>(Action<TEvent> callback)
        where TEvent : IEtlEvent
    {
        var handler = new DelegateEventHandler<TEvent>(callback);
        Subscribe(handler);
        return handler;
    }

    public async Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IEtlEvent
    {
        if (!_handlers.TryGetValue(typeof(TEvent), out var handlers))
            return;

        // Snapshot to avoid issues if a handler modifies subscriptions
        var snapshot = handlers.OfType<IEventHandler<TEvent>>().ToArray();
        var tasks = snapshot.Select(h => h.HandleAsync(@event, cancellationToken));
        await Task.WhenAll(tasks);
    }
}
