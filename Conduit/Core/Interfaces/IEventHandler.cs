using Conduit.Core.Events;

namespace Conduit.Core.Interfaces;

/// <summary>
/// Observer contract. Implement this to react to a specific ETL event type.
/// </summary>
public interface IEventHandler<TEvent> where TEvent : IEtlEvent
{
    Task HandleAsync(TEvent @event, CancellationToken cancellationToken = default);
}
