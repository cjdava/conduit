using System.Diagnostics;
using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.Core.Models;

namespace Conduit.Pipeline;

/// <summary>
/// Row-by-row ETL pipeline that uses plain callback functions for the
/// Transform and Load steps.
///
/// Unlike <see cref="EtlPipeline{TExtracted,TTransformed}"/> which processes records in batches,
/// this pipeline feeds each extracted record individually through the chain:
///
///     extractedRow → transformCallback(row) → loadCallback(transformedRow)
///
/// A <see cref="RowProcessedEvent{T}"/> is published after every row completes the chain,
/// giving observers real-time, per-record visibility.
///
/// Use <see cref="StreamingEtlPipelineBuilder"/> to construct an instance.
/// </summary>
public sealed class StreamingEtlPipeline<TExtracted, TTransformed> : IEtlPipeline
{
    private readonly IStreamingExtractor<TExtracted> _extractor;
    private readonly Func<TExtracted, TTransformed> _transformCallback;
    private readonly Func<TTransformed, CancellationToken, Task> _loadCallback;
    private readonly IEventBus _eventBus;

    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];
    public string Name { get; }

    internal StreamingEtlPipeline(
        string name,
        IStreamingExtractor<TExtracted> extractor,
        Func<TExtracted, TTransformed> transformCallback,
        Func<TTransformed, CancellationToken, Task> loadCallback,
        IEventBus eventBus)
    {
        Name = name;
        _extractor = extractor;
        _transformCallback = transformCallback;
        _loadCallback = loadCallback;
        _eventBus = eventBus;
    }

    public async Task<PipelineResult> RunAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var sw = Stopwatch.StartNew();
        var notifications = new PipelineNotifications(_eventBus);
        var extractedCount = 0;
        var transformedCount = 0;
        var loadedCount = 0;
        await notifications.PublishAsync(new PipelineStartedEvent(Id, Name), cancellationToken);

        try
        {
            // ── Row-by-row: extract → transform → load, one record at a time ──────
            // No list is ever built. Each record flows through the full chain before
            // the next one is read from the source.
            var rowIndex = 0;

            await foreach (var row in _extractor.ExtractAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();
                extractedCount++;
                // 1. Transform callback — called for this row only
                var transformed = _transformCallback(row);
                transformedCount++;

                // 2. Load callback — called immediately; transformed value is discarded after
                cancellationToken.ThrowIfCancellationRequested();
                await _loadCallback(transformed, cancellationToken);
                loadedCount++;

                // 3. Notify observers about this specific row
                await notifications.PublishAsync(
                    new RowProcessedEvent<TTransformed>(Id, transformed, rowIndex), cancellationToken);

                rowIndex++;
            }

            sw.Stop();
            await notifications.PublishAsync(new PipelineCompletedEvent(Id, sw.Elapsed, loadedCount), cancellationToken);

            return PipelineResult.Success(extractedCount, transformedCount, loadedCount, sw.Elapsed, notifications.Errors);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            await notifications.PublishAsync(
                new PipelineFailedEvent(Id, ex, "Transform+Load", sw.Elapsed), cancellationToken);
            return PipelineResult.Failure(ex, "Transform+Load", sw.Elapsed, extractedCount, transformedCount, loadedCount, notifications.Errors);
        }
    }
}

