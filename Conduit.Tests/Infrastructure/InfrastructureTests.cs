using Conduit.Extractors;
using Conduit.Loaders;

namespace Conduit.Tests.Infrastructure;

public sealed class InMemoryExtractorTests
{
    [Fact]
    public async Task ExtractAsync_ReturnsAllSuppliedRecords()
    {
        var extractor = new InMemoryExtractor<int>([10, 20, 30], "TestSource");

        var result = await extractor.ExtractAsync();

        Assert.Equal([10, 20, 30], result);
    }

    [Fact]
    public void Source_ReturnsSuppliedSourceName()
    {
        var extractor = new InMemoryExtractor<string>([], "MySource");

        Assert.Equal("MySource", extractor.Source);
    }

    [Fact]
    public async Task ExtractAsync_ReturnsEmpty_WhenNoRecordsSupplied()
    {
        var extractor = new InMemoryExtractor<int>([], "Empty");

        var result = await extractor.ExtractAsync();

        Assert.Empty(result);
    }
}

public sealed class InMemoryLoaderTests
{
    [Fact]
    public async Task LoadAsync_StoresAllRecords()
    {
        var loader = new InMemoryLoader<string>();

        await loader.LoadAsync(["a", "b", "c"]);

        Assert.Equal(["a", "b", "c"], loader.LoadedRecords);
    }

    [Fact]
    public async Task LoadAsync_ReturnsRecordCount()
    {
        var loader = new InMemoryLoader<int>();

        var count = await loader.LoadAsync([1, 2, 3, 4]);

        Assert.Equal(4, count);
    }

    [Fact]
    public async Task LoadAsync_AccumulatesAcrossMultipleCalls()
    {
        var loader = new InMemoryLoader<int>();

        await loader.LoadAsync([1, 2]);
        await loader.LoadAsync([3, 4]);

        Assert.Equal([1, 2, 3, 4], loader.LoadedRecords);
    }
}
