namespace Conduit.Core.Events;

/// <summary>
/// Published when any individual ETL stage (Extract / Transform / Load) finishes.
/// Provides a non-generic hook for observers such as loggers and metrics collectors.
/// </summary>
public sealed class StageCompletedEvent : EtlEventBase
{
    public string StageName { get; }
    public int RecordsProcessed { get; }

    /// <summary>
    /// Name of the source (Extract) or destination (Load). Null for Transform.
    /// </summary>
    public string? SourceOrDestination { get; }

    public StageCompletedEvent(
        string pipelineId,
        string stageName,
        int recordsProcessed,
        string? sourceOrDestination = null)
        : base(pipelineId)
    {
        StageName = stageName;
        RecordsProcessed = recordsProcessed;
        SourceOrDestination = sourceOrDestination;
    }
}
