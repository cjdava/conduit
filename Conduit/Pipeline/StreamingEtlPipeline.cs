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
        var sw = Stopwatch.StartNew();
        await _eventBus.PublishAsync(new PipelineStartedEvent(Id, Name), cancellationToken);

        try
        {
            // ── Row-by-row: extract → transform → load, one record at a time ──────
            // No list is ever built. Each record flows through the full chain before
            // the next one is read from the source.
            var rowIndex = 0;
            var loadedCount = 0;

            await foreach (var row in _extractor.ExtractAsync(cancellationToken))
            {
                // 1. Transform callback — called for this row only
                var transformed = _transformCallback(row);

                // 2. Load callback — called immediately; transformed value is discarded after
                await _loadCallback(transformed, cancellationToken);
                loadedCount++;

                // 3. Notify observers about this specific row
                await _eventBus.PublishAsync(
                    new RowProcessedEvent<TTransformed>(Id, transformed, rowIndex), cancellationToken);

                rowIndex++;
            }

            sw.Stop();
            var result = PipelineResult.Success(rowIndex, rowIndex, loadedCount, sw.Elapsed);
            await _eventBus.PublishAsync(new PipelineCompletedEvent(Id, sw.Elapsed, loadedCount), cancellationToken);

            return result;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            await _eventBus.PublishAsync(
                new PipelineFailedEvent(Id, ex, "Transform+Load", sw.Elapsed), cancellationToken);
            return PipelineResult.Failure(ex, "Transform+Load", sw.Elapsed);
        }
    }
}
