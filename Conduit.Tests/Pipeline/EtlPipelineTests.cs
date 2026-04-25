using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.EventBus;
using Conduit.Extractors;
using Conduit.Loaders;
using Conduit.Pipeline;
using Conduit.Transformers;
using NSubstitute;

namespace Conduit.Tests.Pipeline;

public sealed class EtlPipelineTests
{
    private readonly InMemoryEventBus _bus = new();

    [Fact]
    public async Task RunAsync_ReturnsSuccess_WhenAllStagesSucceed()
    {
        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2, 3], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new FilterTransformer<int>(_ => true))
            .WithLoader(new InMemoryLoader<int>());

        var result = await pipeline.RunAsync();

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.RecordsExtracted);
        Assert.Equal(3, result.RecordsTransformed);
        Assert.Equal(3, result.RecordsLoaded);
    }

    [Fact]
    public async Task RunAsync_ReturnsFailure_WhenTransformThrows()
    {
        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2, 3], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new DelegateTransformer<int, int>(_ => throw new InvalidOperationException("boom")))
            .WithLoader(new InMemoryLoader<int>());

        var result = await pipeline.RunAsync();

        Assert.False(result.IsSuccess);
        Assert.Equal("Transform", result.FailedStage);
        Assert.IsType<InvalidOperationException>(result.Error);
    }

    [Fact]
    public async Task RunAsync_PublishesPipelineStartedEvent()
    {
        var handler = Substitute.For<IEventHandler<PipelineStartedEvent>>();
        _bus.Subscribe(handler);

        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new FilterTransformer<int>(_ => true))
            .WithLoader(new InMemoryLoader<int>());

        await pipeline.RunAsync();

        await handler.Received(1).HandleAsync(
            Arg.Is<PipelineStartedEvent>(e => e.PipelineName == "EtlPipeline"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_PublishesPipelineCompletedEvent_OnSuccess()
    {
        var handler = Substitute.For<IEventHandler<PipelineCompletedEvent>>();
        _bus.Subscribe(handler);

        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new FilterTransformer<int>(_ => true))
            .WithLoader(new InMemoryLoader<int>());

        await pipeline.RunAsync();

        await handler.Received(1).HandleAsync(
            Arg.Is<PipelineCompletedEvent>(e => e.TotalRecordsLoaded == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_PublishesPipelineFailedEvent_OnError()
    {
        var handler = Substitute.For<IEventHandler<PipelineFailedEvent>>();
        _bus.Subscribe(handler);

        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new DelegateTransformer<int, int>(_ => throw new Exception("err")))
            .WithLoader(new InMemoryLoader<int>());

        await pipeline.RunAsync();

        await handler.Received(1).HandleAsync(
            Arg.Is<PipelineFailedEvent>(e => e.FailedStage == "Transform"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_PublishesThreeStageCompletedEvents_OnSuccess()
    {
        var events = new List<StageCompletedEvent>();
        var handler = Substitute.For<IEventHandler<StageCompletedEvent>>();
        handler
            .When(h => h.HandleAsync(Arg.Any<StageCompletedEvent>(), Arg.Any<CancellationToken>()))
            .Do(ci => events.Add(ci.Arg<StageCompletedEvent>()));
        _bus.Subscribe(handler);

        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([10, 20], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new FilterTransformer<int>(_ => true))
            .WithLoader(new InMemoryLoader<int>());

        await pipeline.RunAsync();

        Assert.Equal(3, events.Count);
        Assert.Equal("Extract", events[0].StageName);
        Assert.Equal("Transform", events[1].StageName);
        Assert.Equal("Load", events[2].StageName);
    }

    [Fact]
    public async Task RunAsync_FiltersRecordsCorrectly()
    {
        var loader = new InMemoryLoader<int>();

        var pipeline = EtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2, 3, 4, 5], "Test"))
            .WithEventBus(_bus)
            .WithTransformer(new FilterTransformer<int>(n => n % 2 == 0))
            .WithLoader(loader);

        await pipeline.RunAsync();

        Assert.Equal([2, 4], loader.LoadedRecords);
    }
}
