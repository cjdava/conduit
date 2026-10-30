using Conduit.Core.Interfaces;

namespace Conduit.Transformers;

/// <summary>
/// Transforms records by applying a user-supplied mapping function to every element.
/// </summary>
public sealed class DelegateTransformer<TIn, TOut> : ITransformer<TIn, TOut>
{
    private readonly Func<TIn, TOut> _map;

    public DelegateTransformer(Func<TIn, TOut> map)
    {
        _map = map;
    }

    public Task<IReadOnlyList<TOut>> TransformAsync(
        IReadOnlyList<TIn> records, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = new List<TOut>();
        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result.Add(_map(record));
        }
        return Task.FromResult<IReadOnlyList<TOut>>(result);
    }
}

