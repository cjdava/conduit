using Conduit.Transformers;

namespace Conduit.Tests.Transformers;

public sealed class FilterTransformerTests
{
    [Fact]
    public async Task TransformAsync_KeepsRecordsMatchingPredicate()
    {
        var transformer = new FilterTransformer<int>(n => n > 3);
        var input = new[] { 1, 2, 3, 4, 5 };

        var result = await transformer.TransformAsync(input);

        Assert.Equal([4, 5], result);
    }

    [Fact]
    public async Task TransformAsync_ReturnsEmpty_WhenNoneMatch()
    {
        var transformer = new FilterTransformer<int>(_ => false);
        var input = new[] { 1, 2, 3 };

        var result = await transformer.TransformAsync(input);

        Assert.Empty(result);
    }

    [Fact]
    public async Task TransformAsync_ReturnsAll_WhenAllMatch()
    {
        var transformer = new FilterTransformer<string>(s => s.Length > 0);
        var input = new[] { "a", "bb", "ccc" };

        var result = await transformer.TransformAsync(input);

        Assert.Equal(input, result);
    }
}

public sealed class DelegateTransformerTests
{
    [Fact]
    public async Task TransformAsync_AppliesMappingToAllRecords()
    {
        var transformer = new DelegateTransformer<int, string>(n => $"item-{n}");
        var input = new[] { 1, 2, 3 };

        var result = await transformer.TransformAsync(input);

        Assert.Equal(["item-1", "item-2", "item-3"], result);
    }

    [Fact]
    public async Task TransformAsync_ReturnsEmpty_WhenInputIsEmpty()
    {
        var transformer = new DelegateTransformer<int, int>(n => n * 2);

        var result = await transformer.TransformAsync([]);

        Assert.Empty(result);
    }
}

public sealed class CompositeTransformerTests
{
    [Fact]
    public async Task TransformAsync_AppliesTransformersInOrder()
    {
        var steps = new[]
        {
            new FilterTransformer<int>(n => n > 1),   // removes 1     → [2,3,4,5]
            new FilterTransformer<int>(n => n < 5),   // removes 5     → [2,3,4]
        };
        var composite = new CompositeTransformer<int>(steps);

        var result = await composite.TransformAsync([1, 2, 3, 4, 5]);

        Assert.Equal([2, 3, 4], result);
    }
}

public sealed class SequentialTransformerTests
{
    [Fact]
    public async Task TransformAsync_ChainsFilterThenMap()
    {
        var filter = new FilterTransformer<int>(n => n % 2 == 0);
        var map = new DelegateTransformer<int, string>(n => $"even-{n}");
        var seq = new SequentialTransformer<int, int, string>(filter, map);

        var result = await seq.TransformAsync([1, 2, 3, 4, 5]);

        Assert.Equal(["even-2", "even-4"], result);
    }
}
