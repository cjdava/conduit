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
    private readonly IExtractor<TExtracted> _extractor;
    private readonly Func<TExtracted, TTransformed> _transformCallback;
    private readonly Func<TTransformed, CancellationToken, Task> _loadCallback;
    private readonly IEventBus _eventBus;

    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];
    public string Name { get; }

    internal StreamingEtlPipeline(
        string name,
        IExtractor<TExtracted> extractor,
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

        var currentStage = "Extract";
        try
        {
            // ── Extract all records first ──────────────────────────────────────────
            var records = await _extractor.ExtractAsync(cancellationToken);
            await _eventBus.PublishAsync(
                new StageCompletedEvent(Id, "Extract", records.Count, _extractor.Source), cancellationToken);

            // ── Row-by-row: transform callback → load callback ─────────────────────
            currentStage = "Transform+Load";
            var loadedCount = 0;

            for (var i = 0; i < records.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // 1. Transform callback — called for every row
                var transformed = _transformCallback(records[i]);

                // 2. Load callback — called immediately after the row is transformed
                await _loadCallback(transformed, cancellationToken);

                loadedCount++;

                // 3. Notify observers about this specific row
                await _eventBus.PublishAsync(
                    new RowProcessedEvent<TTransformed>(Id, transformed, i), cancellationToken);
            }

            sw.Stop();
            var result = PipelineResult.Success(records.Count, records.Count, loadedCount, sw.Elapsed);
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
                new PipelineFailedEvent(Id, ex, currentStage, sw.Elapsed), cancellationToken);
            return PipelineResult.Failure(ex, currentStage, sw.Elapsed);
        }
    }
}
