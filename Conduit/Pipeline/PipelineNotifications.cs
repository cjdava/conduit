using Conduit.Core.Events;
using Conduit.Core.Interfaces;

namespace Conduit.Pipeline;

/// <summary>Collects observer errors without changing the processing outcome.</summary>
internal sealed class PipelineNotifications(IEventBus eventBus)
{
    public List<Exception> Errors { get; } = new();

    public async Task PublishAsync<TEvent>(TEvent notification, CancellationToken cancellationToken)
        where TEvent : IEtlEvent
    {
        try
        {
            await eventBus.PublishAsync(notification, cancellationToken);
        }
        catch (Exception error)
        {
            // Cancellation is checked at processing boundaries. A notification failure
            // after a successful load must not turn committed work into a failed run.
            Errors.Add(error);
        }
    }
}
