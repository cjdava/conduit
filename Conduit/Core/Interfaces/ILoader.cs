namespace Conduit.Core.Interfaces;

/// <summary>
/// Writes transformed records to a destination. Returns the number of records persisted.
/// </summary>
public interface ILoader<TIn>
{
    string Destination { get; }
    Task<int> LoadAsync(IReadOnlyList<TIn> records, CancellationToken cancellationToken = default);
}
