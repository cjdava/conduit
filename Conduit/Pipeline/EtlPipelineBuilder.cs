using Conduit.Core.Interfaces;
using Conduit.EventBus;

namespace Conduit.Pipeline;

/// <summary>
/// Entry point for constructing a type-safe ETL pipeline using a fluent API.
///
/// Usage:
/// <code>
/// var pipeline = EtlPipelineBuilder
///     .WithExtractor(new CsvExtractor("data.csv"))
///     .WithName("My Pipeline")
///     .WithEventBus(bus)
///     .WithTransformer(new DelegateTransformer&lt;CsvRow, Product&gt;(r => Map(r)))
///     .WithLoader(new ConsoleLoader&lt;Product&gt;());
///
/// var result = await pipeline.RunAsync();
/// </code>
/// </summary>
public static class EtlPipelineBuilder
{
    public static EtlPipelineBuilder<TExtracted> WithExtractor<TExtracted>(IExtractor<TExtracted> extractor)
        => new(extractor);
}

/// <summary>Builder state after the extractor has been set.</summary>
public sealed class EtlPipelineBuilder<TExtracted>
{
    private readonly IExtractor<TExtracted> _extractor;
    private string _name = "EtlPipeline";
    private IEventBus _eventBus = new InMemoryEventBus();

    internal EtlPipelineBuilder(IExtractor<TExtracted> extractor)
        => _extractor = extractor;

    public EtlPipelineBuilder<TExtracted> WithName(string name)
    {
        _name = name;
        return this;
    }

    public EtlPipelineBuilder<TExtracted> WithEventBus(IEventBus eventBus)
    {
        _eventBus = eventBus;
        return this;
    }

    /// <summary>
    /// Sets the transformer, locking in both the extracted and transformed types.
    /// </summary>
    public EtlPipelineBuilder<TExtracted, TTransformed> WithTransformer<TTransformed>(
        ITransformer<TExtracted, TTransformed> transformer)
        => new(_name, _eventBus, _extractor, transformer);
}

/// <summary>Builder state after extractor and transformer have been set.</summary>
public sealed class EtlPipelineBuilder<TExtracted, TTransformed>
{
    private readonly string _name;
    private readonly IEventBus _eventBus;
    private readonly IExtractor<TExtracted> _extractor;
    private readonly ITransformer<TExtracted, TTransformed> _transformer;

    internal EtlPipelineBuilder(
        string name,
        IEventBus eventBus,
        IExtractor<TExtracted> extractor,
        ITransformer<TExtracted, TTransformed> transformer)
    {
        _name = name;
        _eventBus = eventBus;
        _extractor = extractor;
        _transformer = transformer;
    }

    /// <summary>
    /// Completes the builder and returns a ready-to-run pipeline.
    /// </summary>
    public EtlPipeline<TExtracted, TTransformed> WithLoader(ILoader<TTransformed> loader)
        => new(_name, _extractor, _transformer, loader, _eventBus);
}
