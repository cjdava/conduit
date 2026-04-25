namespace Conduit.Core.Events;

public abstract class EtlEventBase : IEtlEvent
{
    public string PipelineId { get; }
    public DateTime Timestamp { get; }

    protected EtlEventBase(string pipelineId)
    {
        PipelineId = pipelineId;
        Timestamp = DateTime.UtcNow;
    }
}
