using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.EventBus;
using Conduit.Handlers;

namespace Conduit.Tests.Handlers;

public sealed class DelegateEventHandlerTests
{
    [Fact]
    public async Task HandleAsync_InvokesAsyncCallback()
    {
        var called = false;
        var handler = new DelegateEventHandler<PipelineStartedEvent>(
            async (e, _) => { called = true; await Task.CompletedTask; });

        await handler.HandleAsync(new PipelineStartedEvent("p", "P"));

        Assert.True(called);
    }

    [Fact]
    public async Task HandleAsync_InvokesSyncCallback()
    {
        string? received = null;
        var handler = new DelegateEventHandler<PipelineStartedEvent>(
            e => received = e.PipelineName);

        await handler.HandleAsync(new PipelineStartedEvent("p", "MyPipeline"));

        Assert.Equal("MyPipeline", received);
    }

    [Fact]
    public async Task EventBus_LambdaSubscribe_ReceivesEvents()
    {
        var bus = new InMemoryEventBus();
        var receivedIds = new List<string>();

        bus.Subscribe<PipelineStartedEvent>(e => receivedIds.Add(e.PipelineId));

        await bus.PublishAsync(new PipelineStartedEvent("id-1", "P1"));
        await bus.PublishAsync(new PipelineStartedEvent("id-2", "P2"));

        Assert.Equal(["id-1", "id-2"], receivedIds);
    }

    [Fact]
    public async Task EventBus_LambdaSubscribe_ReturnsHandlerThatCanBeUnsubscribed()
    {
        var bus = new InMemoryEventBus();
        var count = 0;

        var handler = bus.Subscribe<PipelineStartedEvent>(e => count++);

        await bus.PublishAsync(new PipelineStartedEvent("p", "P"));
        bus.Unsubscribe(handler);
        await bus.PublishAsync(new PipelineStartedEvent("p", "P"));

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task EventBus_AsyncLambdaSubscribe_ReceivesEvents()
    {
        var bus = new InMemoryEventBus();
        var durations = new List<TimeSpan>();

        bus.Subscribe<PipelineCompletedEvent>(async (e, _) =>
        {
            await Task.Yield();
            durations.Add(e.Duration);
        });

        await bus.PublishAsync(new PipelineCompletedEvent("p", TimeSpan.FromSeconds(1), 5));
        await bus.PublishAsync(new PipelineCompletedEvent("p", TimeSpan.FromSeconds(2), 3));

        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)], durations);
    }

    [Fact]
    public async Task ProcessedCount_TracksNumberOfHandledEvents()
    {
        var handler = new DelegateEventHandler<PipelineStartedEvent>(_ => { });

        Assert.Equal(0, handler.ProcessedCount);

        await handler.HandleAsync(new PipelineStartedEvent("p1", "A"));
        await handler.HandleAsync(new PipelineStartedEvent("p2", "B"));
        await handler.HandleAsync(new PipelineStartedEvent("p3", "C"));

        Assert.Equal(3, handler.ProcessedCount);
    }

    [Fact]
    public async Task ProcessedCount_TracksRowsViaEventBusSubscription()
    {
        var bus = new InMemoryEventBus();
        var handler = bus.Subscribe<RowProcessedEvent<int>>(e => { });

        for (var i = 0; i < 5; i++)
            await bus.PublishAsync(new RowProcessedEvent<int>("pipe", i * 10, i));

        Assert.Equal(5, handler.ProcessedCount);
    }
}
