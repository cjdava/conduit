namespace Conduit.Core.Events;

/// <summary>
/// Published when the pipeline encounters an unrecoverable error in any stage.
/// </summary>
public sealed class PipelineFailedEvent : EtlEventBase
{
    public Exception Exception { get; }
    public string FailedStage { get; }
    public TimeSpan Duration { get; }

    public PipelineFailedEvent(string pipelineId, Exception exception, string failedStage, TimeSpan duration)
        : base(pipelineId)
    {
        Exception = exception;
        FailedStage = failedStage;
        Duration = duration;
    }
}
