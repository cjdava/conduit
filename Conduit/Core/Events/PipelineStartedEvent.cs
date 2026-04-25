namespace Conduit.Core.Events;

/// <summary>
/// Published when the pipeline begins execution.
/// </summary>
public sealed class PipelineStartedEvent : EtlEventBase
{
    public string PipelineName { get; }

    public PipelineStartedEvent(string pipelineId, string pipelineName)
        : base(pipelineId)
    {
        PipelineName = pipelineName;
    }
}
