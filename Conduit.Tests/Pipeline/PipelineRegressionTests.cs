using Conduit.Core.Events;
using Conduit.EventBus;
using Conduit.Extractors;
using Conduit.Loaders;
using Conduit.Pipeline;
using Conduit.Transformers;

namespace Conduit.Tests.Pipeline;

public sealed class PipelineRegressionTests
{
    [Fact]
    public async Task CancelledBatchDoesNotWrite()
    {
        var loader = new InMemoryLoader<int>();
        var pipeline = EtlPipelineBuilder.WithExtractor(new InMemoryExtractor<int>([1]))
            .WithTransformer(new FilterTransformer<int>(_ => true)).WithLoader(loader);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pipeline.RunAsync(new CancellationToken(true)));
        Assert.Empty(loader.LoadedRecords);
    }

    [Fact]
    public async Task CancellationDuringTransformPreventsLoad()
    {
        using var cts = new CancellationTokenSource();
        var loader = new InMemoryLoader<int>();
        var pipeline = EtlPipelineBuilder.WithExtractor(new InMemoryExtractor<int>([1, 2]))
            .WithTransformer(new DelegateTransformer<int, int>(n => { cts.Cancel(); return n; }))
            .WithLoader(loader);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pipeline.RunAsync(cts.Token));
        Assert.Empty(loader.LoadedRecords);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletionObserverCannotFailCommittedRun(bool streaming)
    {
        var bus = new InMemoryEventBus();
        var error = new InvalidOperationException("observer");
        bus.Subscribe<PipelineCompletedEvent>(_ => throw error);
        var loader = new InMemoryLoader<int>();
        var result = streaming
            ? await StreamingEtlPipelineBuilder.WithExtractor(new InMemoryStreamingExtractor<int>([1]))
                .WithEventBus(bus).WithTransformCallback(n => n)
                .WithLoadCallback((n, ct) => loader.LoadAsync([n], ct)).RunAsync()
            : await EtlPipelineBuilder.WithExtractor(new InMemoryExtractor<int>([1]))
                .WithEventBus(bus).WithTransformer(new FilterTransformer<int>(_ => true))
                .WithLoader(loader).RunAsync();
        Assert.True(result.IsSuccess);
        Assert.Equal(1, result.RecordsLoaded);
        Assert.Single(loader.LoadedRecords);
        Assert.Same(error, Assert.Single(result.NotificationErrors));
    }

    [Fact]
    public async Task PartialStreamingFailurePreservesProgressAndOriginalError()
    {
        var bus = new InMemoryEventBus();
        var processingError = new InvalidOperationException("bad row");
        var observerError = new Exception("audit unavailable");
        bus.Subscribe<PipelineFailedEvent>(_ => throw observerError);
        var loaded = new List<int>();
        var result = await StreamingEtlPipelineBuilder.WithExtractor(new InMemoryStreamingExtractor<int>([1, 2]))
            .WithEventBus(bus).WithTransformCallback(n => n == 2 ? throw processingError : n)
            .WithLoadCallback(n => loaded.Add(n)).RunAsync();
        Assert.False(result.IsSuccess);
        Assert.Same(processingError, result.Error);
        Assert.Equal(2, result.RecordsExtracted);
        Assert.Equal(1, result.RecordsTransformed);
        Assert.Equal(1, result.RecordsLoaded);
        Assert.Single(loaded);
        Assert.Same(observerError, Assert.Single(result.NotificationErrors));
    }
}
