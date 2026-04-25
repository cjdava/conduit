using Conduit.Core.Interfaces;

namespace Conduit.Extractors;

/// <summary>
/// Extracts records from a CSV file, returning each data row as a
/// <see cref="Dictionary{String, String}"/> keyed by column header.
///
/// Assumes the first line contains comma-separated headers and that values
/// do not themselves contain commas. For production use, replace with a
/// proper CSV library such as CsvHelper.
/// </summary>
public sealed class CsvExtractor : IExtractor<Dictionary<string, string>>
{
    private readonly string _filePath;

    public string Source => _filePath;

    public CsvExtractor(string filePath)
    {
        _filePath = filePath;
    }

    public async Task<IReadOnlyList<Dictionary<string, string>>> ExtractAsync(
        CancellationToken cancellationToken = default)
    {
        var lines = await File.ReadAllLinesAsync(_filePath, cancellationToken);

        if (lines.Length < 2)
            return Array.Empty<Dictionary<string, string>>();

        var headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();
        var records = new List<Dictionary<string, string>>(lines.Length - 1);

        for (var i = 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
                continue;

            var values = lines[i].Split(',');
            var record = new Dictionary<string, string>(headers.Length, StringComparer.OrdinalIgnoreCase);

            for (var j = 0; j < headers.Length; j++)
                record[headers[j]] = j < values.Length ? values[j].Trim() : string.Empty;

            records.Add(record);
        }

        return records;
    }
}
