using Conduit.Core.Interfaces;
using Conduit.EventBus;

namespace Conduit.Pipeline;

/// <summary>
/// Entry point for constructing a <see cref="StreamingEtlPipeline{TExtracted,TTransformed}"/>
/// using a fluent API.
///
/// Usage:
/// <code>
/// var pipeline = StreamingEtlPipelineBuilder
///     .WithExtractor(new CsvExtractor("data.csv"))
///     .WithName("My Streaming Pipeline")
///     .WithEventBus(bus)
///     .WithTransformCallback(row => MapToProduct(row))
///     .WithLoadCallback(product => Console.WriteLine(product));
/// </code>
/// </summary>
public static class StreamingEtlPipelineBuilder
{
    public static StreamingEtlPipelineBuilder<TExtracted> WithExtractor<TExtracted>(
        IStreamingExtractor<TExtracted> extractor)
        => new(extractor);
}

/// <summary>Builder state after the extractor has been set.</summary>
public sealed class StreamingEtlPipelineBuilder<TExtracted>
{
    private readonly IStreamingExtractor<TExtracted> _extractor;
    private string _name = "StreamingEtlPipeline";
    private IEventBus _eventBus = new InMemoryEventBus();

    internal StreamingEtlPipelineBuilder(IStreamingExtractor<TExtracted> extractor)
        => _extractor = extractor;

    public StreamingEtlPipelineBuilder<TExtracted> WithName(string name)
    {
        _name = name;
        return this;
    }

    public StreamingEtlPipelineBuilder<TExtracted> WithEventBus(IEventBus eventBus)
    {
        _eventBus = eventBus;
        return this;
    }

    /// <summary>
    /// Sets the per-row transform callback, locking in both type parameters.
    /// </summary>
    public StreamingEtlPipelineBuilder<TExtracted, TTransformed> WithTransformCallback<TTransformed>(
        Func<TExtracted, TTransformed> transformCallback)
        => new(_name, _eventBus, _extractor, transformCallback);
}

/// <summary>Builder state after extractor and transform callback have been set.</summary>
public sealed class StreamingEtlPipelineBuilder<TExtracted, TTransformed>
{
    private readonly string _name;
    private readonly IEventBus _eventBus;
    private readonly IStreamingExtractor<TExtracted> _extractor;
    private readonly Func<TExtracted, TTransformed> _transformCallback;

    internal StreamingEtlPipelineBuilder(
        string name,
        IEventBus eventBus,
        IStreamingExtractor<TExtracted> extractor,
        Func<TExtracted, TTransformed> transformCallback)
    {
        _name = name;
        _eventBus = eventBus;
        _extractor = extractor;
        _transformCallback = transformCallback;
    }

    /// <summary>Async load callback. Returns the ready-to-run pipeline.</summary>
    public StreamingEtlPipeline<TExtracted, TTransformed> WithLoadCallback(
        Func<TTransformed, CancellationToken, Task> loadCallback)
        => new(_name, _extractor, _transformCallback, loadCallback, _eventBus);

    /// <summary>Synchronous load callback overload.</summary>
    public StreamingEtlPipeline<TExtracted, TTransformed> WithLoadCallback(
        Action<TTransformed> loadCallback)
        => new(_name, _extractor, _transformCallback,
            (r, _) => { loadCallback(r); return Task.CompletedTask; },
            _eventBus);
}
