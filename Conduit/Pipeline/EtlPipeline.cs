using System.Diagnostics;
using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.Core.Models;

namespace Conduit.Pipeline;

/// <summary>
/// Orchestrates a full ETL run: Extract → Transform → Load.
///
/// After each stage the pipeline publishes two events to the event bus:
///   1. A typed data event (<see cref="DataExtractedEvent{T}"/> / <see cref="DataTransformedEvent{T}"/>)
///      so downstream observers can inspect the actual records.
///   2. A <see cref="StageCompletedEvent"/> so non-generic observers (loggers, monitors) receive a
///      uniform notification without caring about the concrete data type.
///
/// Observers register themselves on the <see cref="IEventBus"/> before calling <see cref="RunAsync"/>.
/// This is the core of the Observer Pattern: the pipeline (subject) notifies all registered
/// handlers (observers) without knowing who they are.
/// </summary>
public sealed class EtlPipeline<TExtracted, TTransformed> : IEtlPipeline
{
    private readonly IExtractor<TExtracted> _extractor;
    private readonly ITransformer<TExtracted, TTransformed> _transformer;
    private readonly ILoader<TTransformed> _loader;
    private readonly IEventBus _eventBus;

    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];
    public string Name { get; }

    internal EtlPipeline(
        string name,
        IExtractor<TExtracted> extractor,
        ITransformer<TExtracted, TTransformed> transformer,
        ILoader<TTransformed> loader,
        IEventBus eventBus)
    {
        Name = name;
        _extractor = extractor;
        _transformer = transformer;
        _loader = loader;
        _eventBus = eventBus;
    }

    public async Task<PipelineResult> RunAsync(CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        await _eventBus.PublishAsync(new PipelineStartedEvent(Id, Name), cancellationToken);

        var currentStage = "Extract";
        try
        {
            // ── Extract ────────────────────────────────────────────────────────────
            currentStage = "Extract";
            var extracted = await _extractor.ExtractAsync(cancellationToken);
            await _eventBus.PublishAsync(
                new DataExtractedEvent<TExtracted>(Id, extracted, _extractor.Source), cancellationToken);
            await _eventBus.PublishAsync(
                new StageCompletedEvent(Id, "Extract", extracted.Count, _extractor.Source), cancellationToken);

            // ── Transform ──────────────────────────────────────────────────────────
            currentStage = "Transform";
            var transformed = await _transformer.TransformAsync(extracted, cancellationToken);
            await _eventBus.PublishAsync(
                new DataTransformedEvent<TTransformed>(Id, transformed), cancellationToken);
            await _eventBus.PublishAsync(
                new StageCompletedEvent(Id, "Transform", transformed.Count), cancellationToken);

            // ── Load ───────────────────────────────────────────────────────────────
            currentStage = "Load";
            var loaded = await _loader.LoadAsync(transformed, cancellationToken);
            await _eventBus.PublishAsync(
                new StageCompletedEvent(Id, "Load", loaded, _loader.Destination), cancellationToken);

            sw.Stop();
            var result = PipelineResult.Success(extracted.Count, transformed.Count, loaded, sw.Elapsed);
            await _eventBus.PublishAsync(
                new PipelineCompletedEvent(Id, sw.Elapsed, loaded), cancellationToken);

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
