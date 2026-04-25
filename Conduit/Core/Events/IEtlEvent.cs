namespace Conduit.Core.Events;

public interface IEtlEvent
{
    string PipelineId { get; }
    DateTime Timestamp { get; }
}
