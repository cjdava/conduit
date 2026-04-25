namespace Conduit.Core.Events;

/// <summary>
/// Published when the pipeline completes all three stages successfully.
/// </summary>
public sealed class PipelineCompletedEvent : EtlEventBase
{
    public TimeSpan Duration { get; }
    public int TotalRecordsLoaded { get; }

    public PipelineCompletedEvent(string pipelineId, TimeSpan duration, int totalRecordsLoaded)
        : base(pipelineId)
    {
        Duration = duration;
        TotalRecordsLoaded = totalRecordsLoaded;
    }
}
