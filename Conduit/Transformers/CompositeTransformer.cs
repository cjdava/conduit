using Conduit.Core.Interfaces;

namespace Conduit.Transformers;

/// <summary>
/// Chains multiple same-type transformers sequentially.
/// Each transformer receives the output of the previous one.
/// Useful for building multi-step transformation pipelines of the same record type.
/// </summary>
public sealed class CompositeTransformer<T> : ITransformer<T, T>
{
    private readonly IReadOnlyList<ITransformer<T, T>> _transformers;

    public CompositeTransformer(IEnumerable<ITransformer<T, T>> transformers)
    {
        _transformers = transformers.ToList();
    }

    public async Task<IReadOnlyList<T>> TransformAsync(
        IReadOnlyList<T> records, CancellationToken cancellationToken = default)
    {
        var current = records;
        foreach (var transformer in _transformers)
            current = await transformer.TransformAsync(current, cancellationToken);

        return current;
    }
}
