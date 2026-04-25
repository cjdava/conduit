using Conduit.Core.Events;
using Conduit.Handlers;

namespace Conduit.Core.Interfaces;

/// <summary>
/// Observable subject. Manages subscriptions and dispatches events to registered handlers.
///
/// Supports two subscription styles:
///   1. Interface-based: implement <see cref="IEventHandler{TEvent}"/> and call <see cref="Subscribe{TEvent}(IEventHandler{TEvent})"/>.
///   2. Callback-based: pass a lambda directly via the Func / Action overloads.
///      Both return the created handler so you can unsubscribe later if needed.
/// </summary>
public interface IEventBus
{
    // ── Interface-based (observer pattern) ────────────────────────────────────
    void Subscribe<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEtlEvent;
    void Unsubscribe<TEvent>(IEventHandler<TEvent> handler) where TEvent : IEtlEvent;

    // ── Callback-based (lambda / delegate) ────────────────────────────────────
    /// <summary>Registers an async callback. Returns the concrete handler, giving access to <see cref="DelegateEventHandler{TEvent}.ProcessedCount"/> and the ability to unsubscribe.</summary>
    DelegateEventHandler<TEvent> Subscribe<TEvent>(Func<TEvent, CancellationToken, Task> callback) where TEvent : IEtlEvent;

    /// <summary>Registers a synchronous callback. Returns the concrete handler, giving access to <see cref="DelegateEventHandler{TEvent}.ProcessedCount"/> and the ability to unsubscribe.</summary>
    DelegateEventHandler<TEvent> Subscribe<TEvent>(Action<TEvent> callback) where TEvent : IEtlEvent;

    Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IEtlEvent;
}
