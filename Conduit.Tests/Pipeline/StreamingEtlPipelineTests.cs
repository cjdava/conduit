using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.EventBus;
using Conduit.Extractors;
using Conduit.Handlers;
using Conduit.Pipeline;

namespace Conduit.Tests.Pipeline;

public sealed class StreamingEtlPipelineTests
{
    private static InMemoryEventBus MakeBus() => new();

    [Fact]
    public async Task RunAsync_CallsTransformCallbackForEveryRow()
    {
        var transformed = new List<int>();
        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2, 3]))
            .WithTransformCallback(n => { transformed.Add(n); return n * 10; })
            .WithLoadCallback(_ => { });

        await pipeline.RunAsync();

        Assert.Equal([1, 2, 3], transformed);
    }

    [Fact]
    public async Task RunAsync_CallsLoadCallbackForEveryTransformedRow()
    {
        var loaded = new List<string>();
        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2, 3]))
            .WithTransformCallback(n => $"item-{n}")
            .WithLoadCallback((string s) => loaded.Add(s));

        await pipeline.RunAsync();

        Assert.Equal(["item-1", "item-2", "item-3"], loaded);
    }

    [Fact]
    public async Task RunAsync_PublishesRowProcessedEventPerRow()
    {
        var bus = MakeBus();
        var rowEvents = new List<RowProcessedEvent<int>>();
        bus.Subscribe<RowProcessedEvent<int>>(e => rowEvents.Add(e));

        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([10, 20, 30]))
            .WithEventBus(bus)
            .WithTransformCallback(n => n)
            .WithLoadCallback(_ => { });

        await pipeline.RunAsync();

        Assert.Equal(3, rowEvents.Count);
        Assert.Equal(0, rowEvents[0].RowIndex);
        Assert.Equal(1, rowEvents[1].RowIndex);
        Assert.Equal(2, rowEvents[2].RowIndex);
        Assert.Equal([10, 20, 30], rowEvents.Select(e => e.Row));
    }

    [Fact]
    public async Task RunAsync_PublishesPipelineStartedAndCompleted()
    {
        var bus = MakeBus();
        PipelineStartedEvent? started = null;
        PipelineCompletedEvent? completed = null;
        bus.Subscribe<PipelineStartedEvent>(e => started = e);
        bus.Subscribe<PipelineCompletedEvent>(e => completed = e);

        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1]))
            .WithName("TestPipeline")
            .WithEventBus(bus)
            .WithTransformCallback(n => n)
            .WithLoadCallback(_ => { });

        await pipeline.RunAsync();

        Assert.NotNull(started);
        Assert.Equal("TestPipeline", started.PipelineName);
        Assert.NotNull(completed);
        Assert.Equal(1, completed.TotalRecordsLoaded);
    }

    [Fact]
    public async Task RunAsync_ReturnsFailure_WhenTransformCallbackThrows()
    {
        var bus = MakeBus();
        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1, 2]))
            .WithEventBus(bus)
            .WithTransformCallback<int>(n =>
            {
                if (n == 2) throw new InvalidOperationException("bad row");
                return n;
            })
            .WithLoadCallback(_ => { });

        var result = await pipeline.RunAsync();

        Assert.False(result.IsSuccess);
        Assert.Equal("Transform+Load", result.FailedStage);
    }

    [Fact]
    public async Task RunAsync_PublishesPipelineFailedEvent_OnError()
    {
        var bus = MakeBus();
        PipelineFailedEvent? failed = null;
        bus.Subscribe<PipelineFailedEvent>(e => failed = e);

        var pipeline = StreamingEtlPipelineBuilder
            .WithExtractor(new InMemoryExtractor<int>([1]))
            .WithEventBus(bus)
            .WithTransformCallback<int>(_ => throw new Exception("boom"))
            .WithLoadCallback(_ => { });

        await pipeline.RunAsync();

        Assert.NotNull(failed);
        Assert.Equal("boom", failed.Exception.Message);
    }
}
