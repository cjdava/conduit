using System.Runtime.CompilerServices;
using Conduit.Core.Interfaces;

namespace Conduit.Extractors;

/// <summary>
/// Streaming extractor backed by an in-memory sequence.
/// Yields each record individually so the rest of the pipeline can begin
/// processing immediately — useful for testing and demos.
/// </summary>
public sealed class InMemoryStreamingExtractor<T> : IStreamingExtractor<T>
{
    private readonly IEnumerable<T> _records;

    public string Source { get; }

    public InMemoryStreamingExtractor(IEnumerable<T> records, string source = "InMemory")
    {
        _records = records;
        Source = source;
    }

#pragma warning disable CS1998 // no real I/O here; async iterator required by the interface
    public async IAsyncEnumerable<T> ExtractAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var record in _records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return record;
        }
    }
#pragma warning restore CS1998
}
