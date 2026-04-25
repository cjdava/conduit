namespace Conduit.Core.Interfaces;

/// <summary>
/// Reads raw records from a data source.
/// </summary>
public interface IExtractor<TOut>
{
    string Source { get; }
    Task<IReadOnlyList<TOut>> ExtractAsync(CancellationToken cancellationToken = default);
}
