namespace Conduit.Core.Events;

/// <summary>
/// Published after the Transform stage completes, carrying the transformed records.
/// Subscribe to this to inspect, validate, or branch on transformed data.
/// </summary>
public sealed class DataTransformedEvent<T> : EtlEventBase
{
    public IReadOnlyList<T> Records { get; }

    public DataTransformedEvent(string pipelineId, IReadOnlyList<T> records)
        : base(pipelineId)
    {
        Records = records;
    }
}
