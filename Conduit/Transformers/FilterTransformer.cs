using Conduit.Core.Interfaces;

namespace Conduit.Transformers;

/// <summary>
/// Keeps only records that satisfy the supplied predicate.
/// Both input and output types are the same (TIn = TOut = T).
/// </summary>
public sealed class FilterTransformer<T> : ITransformer<T, T>
{
    private readonly Func<T, bool> _predicate;

    public FilterTransformer(Func<T, bool> predicate)
    {
        _predicate = predicate;
    }

    public Task<IReadOnlyList<T>> TransformAsync(
        IReadOnlyList<T> records, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<T> result = records.Where(_predicate).ToList();
        return Task.FromResult(result);
    }
}
