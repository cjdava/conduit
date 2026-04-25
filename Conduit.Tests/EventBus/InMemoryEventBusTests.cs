using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.EventBus;
using NSubstitute;

namespace Conduit.Tests.EventBus;

public sealed class InMemoryEventBusTests
{
    [Fact]
    public async Task PublishAsync_CallsSubscribedHandler()
    {
        var bus = new InMemoryEventBus();
        var handler = Substitute.For<IEventHandler<PipelineStartedEvent>>();
        bus.Subscribe(handler);

        var @event = new PipelineStartedEvent("pipe1", "TestPipeline");
        await bus.PublishAsync(@event);

        await handler.Received(1).HandleAsync(@event, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_CallsAllSubscribedHandlers()
    {
        var bus = new InMemoryEventBus();
        var handler1 = Substitute.For<IEventHandler<StageCompletedEvent>>();
        var handler2 = Substitute.For<IEventHandler<StageCompletedEvent>>();
        bus.Subscribe(handler1);
        bus.Subscribe(handler2);

        var @event = new StageCompletedEvent("pipe1", "Extract", 5);
        await bus.PublishAsync(@event);

        await handler1.Received(1).HandleAsync(@event, Arg.Any<CancellationToken>());
        await handler2.Received(1).HandleAsync(@event, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_DoesNotCallUnsubscribedHandler()
    {
        var bus = new InMemoryEventBus();
        var handler = Substitute.For<IEventHandler<PipelineCompletedEvent>>();
        bus.Subscribe(handler);
        bus.Unsubscribe(handler);

        var @event = new PipelineCompletedEvent("pipe1", TimeSpan.FromMilliseconds(10), 3);
        await bus.PublishAsync(@event);

        await handler.DidNotReceive().HandleAsync(Arg.Any<PipelineCompletedEvent>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_DoesNotThrow_WhenNoHandlersRegistered()
    {
        var bus = new InMemoryEventBus();
        var @event = new PipelineStartedEvent("pipe1", "TestPipeline");

        var exception = await Record.ExceptionAsync(() => bus.PublishAsync(@event));

        Assert.Null(exception);
    }

    [Fact]
    public async Task Subscribe_HandlerOnlyReceivesItsOwnEventType()
    {
        var bus = new InMemoryEventBus();
        var startedHandler = Substitute.For<IEventHandler<PipelineStartedEvent>>();
        var completedHandler = Substitute.For<IEventHandler<PipelineCompletedEvent>>();
        bus.Subscribe(startedHandler);
        bus.Subscribe(completedHandler);

        await bus.PublishAsync(new PipelineStartedEvent("p", "P"));

        await startedHandler.Received(1).HandleAsync(Arg.Any<PipelineStartedEvent>(), Arg.Any<CancellationToken>());
        await completedHandler.DidNotReceive().HandleAsync(Arg.Any<PipelineCompletedEvent>(), Arg.Any<CancellationToken>());
    }
}
