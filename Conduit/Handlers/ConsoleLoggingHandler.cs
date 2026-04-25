using Conduit.Core.Events;
using Conduit.Core.Interfaces;

namespace Conduit.Handlers;

/// <summary>
/// Observer that logs every pipeline lifecycle event to the console.
/// A single handler class can implement multiple <see cref="IEventHandler{TEvent}"/> interfaces,
/// one per event type it wants to observe.
/// </summary>
public sealed class ConsoleLoggingHandler :
    IEventHandler<PipelineStartedEvent>,
    IEventHandler<StageCompletedEvent>,
    IEventHandler<PipelineCompletedEvent>,
    IEventHandler<PipelineFailedEvent>
{
    public Task HandleAsync(PipelineStartedEvent @event, CancellationToken cancellationToken = default)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[{@event.Timestamp:HH:mm:ss.fff}] Pipeline '{@event.PipelineName}' started  (id={@event.PipelineId})");
        Console.ResetColor();
        return Task.CompletedTask;
    }

    public Task HandleAsync(StageCompletedEvent @event, CancellationToken cancellationToken = default)
    {
        var detail = @event.SourceOrDestination is null ? string.Empty : $" [{@event.SourceOrDestination}]";
        Console.WriteLine($"[{@event.Timestamp:HH:mm:ss.fff}] ✓ {$"{@event.StageName}",-10}{detail,-20} records={@event.RecordsProcessed}");
        return Task.CompletedTask;
    }

    public Task HandleAsync(PipelineCompletedEvent @event, CancellationToken cancellationToken = default)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[{@event.Timestamp:HH:mm:ss.fff}] Pipeline completed — loaded={@event.TotalRecordsLoaded} duration={@event.Duration.TotalMilliseconds:F1}ms");
        Console.ResetColor();
        return Task.CompletedTask;
    }

    public Task HandleAsync(PipelineFailedEvent @event, CancellationToken cancellationToken = default)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"[{@event.Timestamp:HH:mm:ss.fff}] Pipeline FAILED at '{@event.FailedStage}': {@event.Exception.Message}");
        Console.ResetColor();
        return Task.CompletedTask;
    }
}
