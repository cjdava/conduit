using Conduit.Core.Events;
using Conduit.Core.Interfaces;

namespace Conduit.Handlers;

/// <summary>
/// Adapts a callback function into an <see cref="IEventHandler{TEvent}"/>.
/// This lets you register plain lambdas with the event bus instead of
/// writing a full handler class:
///
/// <code>
/// bus.Subscribe&lt;PipelineStartedEvent&gt;(e => Console.WriteLine(e.PipelineName));
/// </code>
/// </summary>
public sealed class DelegateEventHandler<TEvent> : IEventHandler<TEvent>
    where TEvent : IEtlEvent
{
    private readonly Func<TEvent, CancellationToken, Task> _callback;
    private int _processedCount;

    /// <summary>Total number of events this handler has processed.</summary>
    public int ProcessedCount => _processedCount;

    /// <summary>Async callback constructor.</summary>
    public DelegateEventHandler(Func<TEvent, CancellationToken, Task> callback)
        => _callback = callback;

    /// <summary>Convenience constructor for synchronous callbacks.</summary>
    public DelegateEventHandler(Action<TEvent> callback)
        => _callback = (e, _) => { callback(e); return Task.CompletedTask; };

    public async Task HandleAsync(TEvent @event, CancellationToken cancellationToken = default)
    {
        await _callback(@event, cancellationToken);
        Interlocked.Increment(ref _processedCount);
    }
}
