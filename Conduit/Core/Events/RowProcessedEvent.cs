namespace Conduit.Core.Events;

/// <summary>
/// Published by <see cref="Pipeline.StreamingEtlPipeline{TExtracted,TTransformed}"/> after each
/// individual record completes the full transform → load cycle.
///
/// Use this when you need per-row visibility (auditing, progress tracking, early validation).
/// For bulk metrics, prefer <see cref="StageCompletedEvent"/> instead.
/// </summary>
public sealed class RowProcessedEvent<T> : EtlEventBase
{
    /// <summary>The fully transformed and loaded record.</summary>
    public T Row { get; }

    /// <summary>0-based index of the row within the current pipeline run.</summary>
    public int RowIndex { get; }

    public RowProcessedEvent(string pipelineId, T row, int rowIndex)
        : base(pipelineId)
    {
        Row = row;
        RowIndex = rowIndex;
    }
}
