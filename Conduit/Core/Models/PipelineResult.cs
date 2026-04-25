namespace Conduit.Core.Models;

/// <summary>
/// Immutable result returned after a pipeline run.
/// </summary>
public sealed class PipelineResult
{
    public bool IsSuccess { get; }
    public int RecordsExtracted { get; }
    public int RecordsTransformed { get; }
    public int RecordsLoaded { get; }
    public TimeSpan Duration { get; }
    public Exception? Error { get; }
    public string? FailedStage { get; }

    private PipelineResult(
        bool isSuccess,
        int recordsExtracted,
        int recordsTransformed,
        int recordsLoaded,
        TimeSpan duration,
        Exception? error,
        string? failedStage)
    {
        IsSuccess = isSuccess;
        RecordsExtracted = recordsExtracted;
        RecordsTransformed = recordsTransformed;
        RecordsLoaded = recordsLoaded;
        Duration = duration;
        Error = error;
        FailedStage = failedStage;
    }

    public static PipelineResult Success(
        int recordsExtracted,
        int recordsTransformed,
        int recordsLoaded,
        TimeSpan duration)
        => new(true, recordsExtracted, recordsTransformed, recordsLoaded, duration, null, null);

    public static PipelineResult Failure(
        Exception error,
        string failedStage,
        TimeSpan duration)
        => new(false, 0, 0, 0, duration, error, failedStage);

    public override string ToString() =>
        IsSuccess
            ? $"Success | Extracted={RecordsExtracted} Transformed={RecordsTransformed} Loaded={RecordsLoaded} Duration={Duration.TotalMilliseconds:F1}ms"
            : $"Failed at {FailedStage} | {Error?.Message} Duration={Duration.TotalMilliseconds:F1}ms";
}
