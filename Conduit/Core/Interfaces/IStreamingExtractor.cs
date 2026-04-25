namespace Conduit.Core.Interfaces;

/// <summary>
/// Extracts records one at a time as an <see cref="IAsyncEnumerable{T}"/> stream.
///
/// Unlike <see cref="IExtractor{TOut}"/>, this interface never materialises a full list —
/// each record is produced (and can be consumed) before the next one is even read from
/// the source. This makes it suitable for large files, network streams, or any source
/// where holding all rows in memory at once would be wasteful.
/// </summary>
public interface IStreamingExtractor<out T>
{
    string Source { get; }

    /// <summary>Returns records one at a time. No buffering occurs internally.</summary>
    IAsyncEnumerable<T> ExtractAsync(CancellationToken cancellationToken = default);
}
