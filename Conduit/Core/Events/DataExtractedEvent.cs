namespace Conduit.Core.Events;

/// <summary>
/// Published after the Extract stage completes, carrying the raw extracted records.
/// Subscribe to this to inspect or branch on extracted data.
/// </summary>
public sealed class DataExtractedEvent<T> : EtlEventBase
{
    public IReadOnlyList<T> Records { get; }
    public string Source { get; }

    public DataExtractedEvent(string pipelineId, IReadOnlyList<T> records, string source)
        : base(pipelineId)
    {
        Records = records;
        Source = source;
    }
}
