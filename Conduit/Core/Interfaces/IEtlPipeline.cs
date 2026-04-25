using Conduit.Core.Models;

namespace Conduit.Core.Interfaces;

/// <summary>
/// Represents an end-to-end ETL pipeline that can be executed.
/// </summary>
public interface IEtlPipeline
{
    string Id { get; }
    string Name { get; }
    Task<PipelineResult> RunAsync(CancellationToken cancellationToken = default);
}
