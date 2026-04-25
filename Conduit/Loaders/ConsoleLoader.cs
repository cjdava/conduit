using Conduit.Core.Interfaces;

namespace Conduit.Loaders;

/// <summary>
/// Loader that writes each record to stdout. Useful for debugging and demos.
/// </summary>
public sealed class ConsoleLoader<T> : ILoader<T>
{
    public string Destination => "Console";

    public Task<int> LoadAsync(IReadOnlyList<T> records, CancellationToken cancellationToken = default)
    {
        foreach (var record in records)
            Console.WriteLine($"  [Load] {record}");

        return Task.FromResult(records.Count);
    }
}
