using Conduit.Core.Interfaces;

namespace Conduit.Transformers;

/// <summary>
/// Chains two transformers of potentially different types: TIn → TMiddle → TOut.
/// Useful when a filter step (same type) is followed by a mapping step (different type).
/// </summary>
public sealed class SequentialTransformer<TIn, TMiddle, TOut> : ITransformer<TIn, TOut>
{
    private readonly ITransformer<TIn, TMiddle> _first;
    private readonly ITransformer<TMiddle, TOut> _second;

    public SequentialTransformer(ITransformer<TIn, TMiddle> first, ITransformer<TMiddle, TOut> second)
    {
        _first = first;
        _second = second;
    }

    public async Task<IReadOnlyList<TOut>> TransformAsync(
        IReadOnlyList<TIn> records, CancellationToken cancellationToken = default)
    {
        var middle = await _first.TransformAsync(records, cancellationToken);
        return await _second.TransformAsync(middle, cancellationToken);
    }
}
