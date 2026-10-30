namespace Conduit.Core.Models;

/// <summary>
/// Immutable result returned after a pipeline run.
/// </summary>
public sealed class PipelineResult
{
    public bool IsSuccess { get; }
    /// <summary>Observer failures, separate from the ETL processing outcome.</summary>
    public IReadOnlyList<Exception> NotificationErrors { get; }
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
        string? failedStage,
        IReadOnlyList<Exception>? notificationErrors)
    {
        IsSuccess = isSuccess;
        NotificationErrors = Array.AsReadOnly(notificationErrors?.ToArray() ?? Array.Empty<Exception>());
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
        TimeSpan duration,
        IReadOnlyList<Exception>? notificationErrors = null)
        => new(true, recordsExtracted, recordsTransformed, recordsLoaded, duration, null, null, notificationErrors);

    public static PipelineResult Failure(
        Exception error,
        string failedStage,
        TimeSpan duration,
        int recordsExtracted = 0,
        int recordsTransformed = 0,
        int recordsLoaded = 0,
        IReadOnlyList<Exception>? notificationErrors = null)
        => new(false, recordsExtracted, recordsTransformed, recordsLoaded, duration, error, failedStage, notificationErrors);

    public override string ToString() =>
        IsSuccess
            ? $"Success | Extracted={RecordsExtracted} Transformed={RecordsTransformed} Loaded={RecordsLoaded} Duration={Duration.TotalMilliseconds:F1}ms"
            : $"Failed at {FailedStage} | {Error?.Message} Duration={Duration.TotalMilliseconds:F1}ms";
}

