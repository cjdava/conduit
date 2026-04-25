using System.Runtime.CompilerServices;
using Conduit.Core.Interfaces;

namespace Conduit.Extractors;

/// <summary>
/// Streaming CSV extractor that reads the source file line by line using a
/// <see cref="StreamReader"/>. Only one line is held in memory at a time — the
/// file is never fully buffered.
///
/// Row 0 is treated as the header; every subsequent non-empty line is yielded as a
/// <see cref="Dictionary{String,String}"/> keyed by column name.
///
/// Limitation: values must not contain commas. For production use replace with
/// a proper CSV library such as CsvHelper.
/// </summary>
public sealed class CsvStreamingExtractor : IStreamingExtractor<Dictionary<string, string>>
{
    private readonly string _filePath;

    public string Source => _filePath;

    public CsvStreamingExtractor(string filePath) => _filePath = filePath;

    public async IAsyncEnumerable<Dictionary<string, string>> ExtractAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var reader = new StreamReader(_filePath);

        var headerLine = await reader.ReadLineAsync(cancellationToken);
        if (headerLine is null) yield break;

        var headers = headerLine.Split(',').Select(h => h.Trim()).ToArray();

        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            var values = line.Split(',');
            var record = new Dictionary<string, string>(headers.Length, StringComparer.OrdinalIgnoreCase);

            for (var j = 0; j < headers.Length; j++)
                record[headers[j]] = j < values.Length ? values[j].Trim() : string.Empty;

            yield return record;
        }
    }
}
