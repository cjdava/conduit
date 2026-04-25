using Conduit.Core.Interfaces;

namespace Conduit.Loaders;

/// <summary>
/// Loader that stores records in an in-memory list.
/// Useful for testing and scenarios where the caller needs to inspect loaded records.
/// </summary>
public sealed class InMemoryLoader<T> : ILoader<T>
{
    private readonly List<T> _store = new();

    public string Destination => "InMemory";

    /// <summary>
    /// All records that have been loaded across one or more pipeline runs.
    /// </summary>
    public IReadOnlyList<T> LoadedRecords => _store.AsReadOnly();

    public Task<int> LoadAsync(IReadOnlyList<T> records, CancellationToken cancellationToken = default)
    {
        _store.AddRange(records);
        return Task.FromResult(records.Count);
    }
}
