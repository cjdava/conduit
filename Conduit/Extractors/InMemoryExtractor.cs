using Conduit.Core.Interfaces;

namespace Conduit.Extractors;

/// <summary>
/// Extractor that returns a pre-supplied list of records. Useful for testing and demos.
/// </summary>
public sealed class InMemoryExtractor<T> : IExtractor<T>
{
    private readonly IReadOnlyList<T> _records;

    public string Source { get; }

    public InMemoryExtractor(IEnumerable<T> records, string source = "InMemory")
    {
        _records = records.ToList().AsReadOnly();
        Source = source;
    }

    public Task<IReadOnlyList<T>> ExtractAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(_records);
}
