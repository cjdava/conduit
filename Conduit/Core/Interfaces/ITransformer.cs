namespace Conduit.Core.Interfaces;

/// <summary>
/// Converts or filters a list of records from one shape into another.
/// </summary>
public interface ITransformer<TIn, TOut>
{
    Task<IReadOnlyList<TOut>> TransformAsync(IReadOnlyList<TIn> records, CancellationToken cancellationToken = default);
}
