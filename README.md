# Conduit

A lightweight, event-driven **Extract → Transform → Load** framework for .NET 10 built on the **Observer Pattern**.

---

## What is it for?

This framework gives you a structured way to move data from one place to another — reading raw records from a source, reshaping them, and writing the result to a destination — while keeping every part of the pipeline loosely coupled through an event bus.

Common use cases:
- Migrating data between systems
- Processing CSV / file exports into a structured store
- Running nightly data sync jobs
- Building audit or reporting pipelines

---

## Project structure

```
ObserverPattern/
├── Conduit/               # Main application
│   ├── Core/
│   │   ├── Events/             # All event types published during a pipeline run
│   │   ├── Interfaces/         # IEventBus, IEventHandler, IExtractor, IStreamingExtractor, ITransformer, ILoader, IConduitPipeline
│   │   └── Models/             # PipelineResult
│   ├── EventBus/
│   │   └── InMemoryEventBus    # In-process event bus (the Observable subject)
│   ├── Pipeline/
│   │   ├── ConduitPipeline             # Batch pipeline — processes records in one go
│   │   ├── ConduitPipelineBuilder      # Fluent builder for ConduitPipeline
│   │   ├── StreamingConduitPipeline    # Row-by-row pipeline — each record triggers callbacks
│   │   └── StreamingConduitPipelineBuilder
│   ├── Extractors/
│   │   ├── InMemoryExtractor           # Returns a pre-supplied list (batch pipelines / testing)
│   │   ├── CsvExtractor                # Reads all rows from a CSV file into a list (batch)
│   │   ├── InMemoryStreamingExtractor  # Yields items one-by-one from an IEnumerable (streaming)
│   │   └── CsvStreamingExtractor       # Reads a CSV file line-by-line — only one row in memory at a time
│   ├── Transformers/
│   │   ├── DelegateTransformer     # Applies a mapping function to every record
│   │   ├── FilterTransformer       # Keeps only records matching a predicate
│   │   ├── CompositeTransformer    # Chains multiple same-type transformers in sequence
│   │   └── SequentialTransformer   # Chains two transformers of different output types (TIn → TMiddle → TOut)
│   ├── Loaders/
│   │   ├── ConsoleLoader       # Writes records to stdout
│   │   └── InMemoryLoader      # Stores records in a list (useful for testing / inspection)
│   ├── Handlers/
│   │   ├── ConsoleLoggingHandler   # Observer: logs all pipeline lifecycle events
│   │   └── DelegateEventHandler    # Wraps a lambda as an IEventHandler
│   └── data/
│       └── employees_1m.csv    # Generated benchmark data (git-ignored — run scripts/generate_csv.py)
└── Conduit.Tests/         # 38 xunit tests
```

---

## How the Observer Pattern is applied

The framework uses the Observer Pattern to decouple the pipeline execution from everything that needs to react to it (logging, auditing, metrics, etc.).

| Pattern role | Implementation |
|---|---|
| **Subject** | `InMemoryEventBus` — maintains a registry of handlers per event type and fans out published events |
| **Observer** | `IEventHandler<TEvent>` — implement this interface to react to a specific event |
| **Events** | Plain C# classes that carry data about what just happened |

The pipeline never calls a logger, a metrics system, or any other concern directly. It only publishes events. Any number of observers can subscribe without the pipeline knowing or caring.

---

## Pipeline flow

### Batch pipeline (`ConduitPipeline`)

All records move through the three stages together.

```
┌─────────────────────────────────────────────────────────────┐
│                   ConduitPipeline.RunAsync()                │
│                                                             │
│  IExtractor.ExtractAsync()                                  │
│       │  publishes: PipelineStartedEvent                    │
│       │             DataExtractedEvent<TExtracted>          │
│       │             StageCompletedEvent (Extract)           │
│       ▼                                                     │
│  ITransformer.TransformAsync(extractedRecords)              │
│       │  publishes: DataTransformedEvent<TTransformed>      │
│       │             StageCompletedEvent (Transform)         │
│       ▼                                                     │
│  ILoader.LoadAsync(transformedRecords)                      │
│       │  publishes: StageCompletedEvent (Load)              │
│       │             PipelineCompletedEvent  ──► success     │
│       │          or PipelineFailedEvent     ──► on error    │
│       ▼                                                     │
│  returns PipelineResult                                     │
└─────────────────────────────────────────────────────────────┘
```

### Streaming pipeline (`StreamingConduitPipeline`)

Each record is extracted, transformed, and loaded **one at a time** — no list is ever built anywhere in the chain. The source yields rows through `IAsyncEnumerable<T>`, so the next row is not even read from the source until the current one has finished loading.

```
┌─────────────────────────────────────────────────────────────┐
│            StreamingConduitPipeline.RunAsync()              │
│                                                             │
│  publishes: PipelineStartedEvent                            │
│                                                             │
│  await foreach row in IStreamingExtractor.ExtractAsync()    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  transformCallback(row)   ──► your delegate/lambda  │   │
│  │  loadCallback(transformed)──► your delegate/lambda  │   │
│  │  publishes: RowProcessedEvent<TTransformed>         │   │
│  └──────────────── repeated per row ───────────────────┘   │
│                                                             │
│  publishes: PipelineCompletedEvent  ──► success             │
│          or PipelineFailedEvent     ──► on error            │
│       ▼                                                     │
│  returns PipelineResult                                     │
└─────────────────────────────────────────────────────────────┘
```

**Memory profile:** only the current row and its transformed value exist on the heap during processing. Every previous row is eligible for garbage collection before the next row is read.

---

## Events reference

| Event | When it fires |
|---|---|
| `PipelineStartedEvent` | Beginning of every pipeline run |
| `StageCompletedEvent` | After Extract, Transform, or Load finishes |
| `DataExtractedEvent<T>` | After Extract, carries the raw records |
| `DataTransformedEvent<T>` | After Transform, carries the transformed records |
| `RowProcessedEvent<T>` | After each individual row in a streaming pipeline — carries the transformed value and its zero-based index |
| `PipelineCompletedEvent` | Successful end of a pipeline run |
| `PipelineFailedEvent` | Unrecoverable error in any stage |

---

## Subscribing to events

### Style A — handler class (reusable, testable)

Implement `IEventHandler<TEvent>`. One class can implement multiple interfaces.

```csharp
public class AuditHandler :
    IEventHandler<PipelineStartedEvent>,
    IEventHandler<PipelineCompletedEvent>
{
    public Task HandleAsync(PipelineStartedEvent e, CancellationToken ct = default)
    {
        Console.WriteLine($"Pipeline '{e.PipelineName}' started");
        return Task.CompletedTask;
    }

    public Task HandleAsync(PipelineCompletedEvent e, CancellationToken ct = default)
    {
        Console.WriteLine($"Loaded {e.TotalRecordsLoaded} records in {e.Duration.TotalMilliseconds:F1}ms");
        return Task.CompletedTask;
    }
}

var bus = new InMemoryEventBus();
var audit = new AuditHandler();
bus.Subscribe((IEventHandler<PipelineStartedEvent>)audit);
bus.Subscribe((IEventHandler<PipelineCompletedEvent>)audit);
```

### Style B — lambda / callback (quick, inline)

Pass a lambda directly. The returned `DelegateEventHandler` exposes `ProcessedCount` and can be passed to `Unsubscribe`.

```csharp
// synchronous lambda
bus.Subscribe<PipelineFailedEvent>(e =>
    Console.WriteLine($"Pipeline {e.PipelineId} failed at '{e.FailedStage}': {e.Exception.Message}"));

// async lambda
bus.Subscribe<PipelineCompletedEvent>(async (e, ct) =>
{
    await SaveToAuditDbAsync(e, ct);
});

// track how many rows were processed
var rowTracker = bus.Subscribe<RowProcessedEvent<string>>(_ => { });
await pipeline.RunAsync();
Console.WriteLine($"Rows processed: {rowTracker.ProcessedCount}");
```

---

## Building and running pipelines

### Batch pipeline

```csharp
var pipeline = ConduitPipelineBuilder
    .WithExtractor(new CsvExtractor("data/employees_1m.csv"))
    .WithName("Senior Engineers Pipeline")
    .WithEventBus(bus)
    .WithTransformer(
        new SequentialTransformer<Dictionary<string, string>, Dictionary<string, string>, string>(
            new FilterTransformer<Dictionary<string, string>>(r => r["Department"] == "Engineering"),
            new DelegateTransformer<Dictionary<string, string>, string>(r => r["Name"])
        ))
    .WithLoader(new ConsoleLoader<string>());

PipelineResult result = await pipeline.RunAsync();
Console.WriteLine(result); // Success | Extracted=8 Transformed=3 Loaded=3 Duration=5ms
```

### Streaming pipeline with per-row callbacks

Use `IStreamingExtractor<T>` as the source — rows are yielded one at a time via `IAsyncEnumerable<T>`, so no full list is ever built.

```csharp
var pipeline = StreamingConduitPipelineBuilder
    .WithExtractor(new CsvStreamingExtractor("orders.csv"))  // line-by-line, no list
    .WithName("Order Import")
    .WithEventBus(bus)
    .WithTransformCallback(row => MapToOrder(row))   // delegate: called per row
    .WithLoadCallback(order => SaveOrder(order));     // delegate: called immediately after

// Observer: reacts to each individual row as it completes
bus.Subscribe<RowProcessedEvent<Order>>(e =>
    Console.WriteLine($"Row #{e.RowIndex} → {e.Row.Id}"));

PipelineResult result = await pipeline.RunAsync();
```

> **Tip:** for in-memory sequences (tests, demos) use `InMemoryStreamingExtractor<T>` instead.

### Using built-in transformers

```csharp
// Filter — keep records matching a condition
new FilterTransformer<int>(n => n > 100)

// Map — convert one type to another
new DelegateTransformer<int, string>(n => $"item-{n}")

// Composite — chain multiple same-type transformers
new CompositeTransformer<int>([
    new FilterTransformer<int>(n => n > 0),
    new FilterTransformer<int>(n => n < 1000)
])

// Sequential — filter step then mapping step (different output type)
new SequentialTransformer<RawRow, RawRow, Product>(filterStep, mapStep)
```

---

## Benchmark CSV data

The benchmark CSV files are **not committed to git** (they range from ~60 MB to ~6 GB). Generate them once with the included Python script before running the demo:

```bash
# From the repo root (conduit/)
python scripts/generate_csv.py
```

This writes four files to `Conduit/data/`:

| File | Rows |
|---|---|
| `employees_1m.csv` | 1,000,000 |
| `employees_10m.csv` | 10,000,000 |
| `employees_50m.csv` | 50,000,000 |
| `employees_100m.csv` | 100,000,000 |

> ⚠️ **Out-of-memory warning:** The **batch** pipeline loads the entire file into a `List<T>` before processing. At large scales this can exhaust available RAM and crash the process with an `OutOfMemoryException`:
> - `employees_50m.csv` (~1.7 GB on disk) requires **~8–10 GB** of heap during the batch run.
> - `employees_100m.csv` (~3.5 GB on disk) requires **~16–20 GB** — it **will OOM** on most consumer machines.
>
> The **streaming** pipeline is not affected — it holds only one row in memory at a time and will complete successfully at any scale.
>
> If your machine has less than 16 GB of free RAM, comment out the larger sizes in `Program.cs` before running, or only run the streaming pipeline for those sizes.

---

## Running the demos

Generate the CSV data first (see above), then:

```bash
# Run both batch and streaming (default)
dotnet run -c Release --project Conduit/Conduit.csproj

# Run only the batch pipeline
dotnet run -c Release --project Conduit/Conduit.csproj -- --batch

# Run only the streaming pipeline
dotnet run -c Release --project Conduit/Conduit.csproj -- --stream
```

The benchmark iterates each enabled CSV file through the selected pipeline(s):
- **Batch** (`CsvExtractor`) — reads the whole file into a list first, then transforms + loads. Memory spikes proportionally to file size.
- **Streaming** (`CsvStreamingExtractor`) — reads line-by-line via `IAsyncEnumerable<T>`; only one row is in memory at a time. Memory stays flat regardless of file size.

If a CSV file is missing the run skips it and prints a reminder to run the generator script.

---

## Integrating with Clean Architecture

Because every moving part of Conduit is hidden behind an interface (`IExtractor<T>`, `ITransformer<TIn,TOut>`, `ILoader<T>`, `IEventBus`), the framework maps directly onto Clean Architecture's dependency rule — outer layers depend inward, never the other way around.

### Layer responsibilities

| Layer | What lives here |
|---|---|
| **Domain** | Your business entities and value objects. Zero knowledge of Conduit. |
| **Application** | Use-case classes that depend only on the Conduit *interfaces*. No concrete types imported. |
| **Infrastructure** | Concrete Conduit implementations: `CsvExtractor`, `SqlLoader`, `SequentialTransformer`, etc. |
| **Composition root** | `Program.cs` or your DI container — wires interfaces to implementations and builds pipelines. |

### Example

**Domain** — pure business entity, no framework dependency:

```csharp
// Domain/Entities/Employee.cs
public record Employee(int Id, string Name, string Department, decimal Salary);
```

**Application** — use case depends only on abstractions:

```csharp
// Application/UseCases/ImportEmployeesUseCase.cs
public class ImportEmployeesUseCase
{
    private readonly IExtractor<Employee> _extractor;
    private readonly ILoader<Employee> _loader;
    private readonly IEventBus _eventBus;

    public ImportEmployeesUseCase(
        IExtractor<Employee> extractor,
        ILoader<Employee> loader,
        IEventBus eventBus)
    {
        _extractor = extractor;
        _loader    = loader;
        _eventBus  = eventBus;
    }

    public Task<PipelineResult> ExecuteAsync(CancellationToken ct = default)
        => EtlPipelineBuilder
            .WithExtractor(_extractor)
            .WithName("Import Employees")
            .WithEventBus(_eventBus)
            .WithTransformer(new DelegateTransformer<Employee, Employee>(e => e))
            .WithLoader(_loader)
            .RunAsync(ct);
}
```

**Infrastructure** — concrete implementations wired up here, invisible to the application layer:

```csharp
// Infrastructure/Extractors/EmployeeCsvExtractor.cs
public sealed class EmployeeCsvExtractor : IExtractor<Employee>
{
    private readonly CsvExtractor _inner;
    public string Source => _inner.Source;

    public EmployeeCsvExtractor(string filePath)
        => _inner = new CsvExtractor(filePath);

    public async Task<IReadOnlyList<Employee>> ExtractAsync(CancellationToken ct = default)
    {
        var rows = await _inner.ExtractAsync(ct);
        return rows.Select(r => new Employee(
            int.Parse(r["Id"]),
            r["Name"],
            r["Department"],
            decimal.Parse(r["Salary"])
        )).ToList();
    }
}

// Infrastructure/Loaders/SqlEmployeeLoader.cs
public sealed class SqlEmployeeLoader : ILoader<Employee>
{
    private readonly string _connectionString;
    public SqlEmployeeLoader(string connectionString) => _connectionString = connectionString;

    public async Task LoadAsync(IReadOnlyList<Employee> records, CancellationToken ct = default)
    {
        // bulk insert via Dapper, EF Core, etc.
    }
}
```

**Composition root** — registers everything and the application layer never changes when you swap implementations:

```csharp
// Program.cs (or Startup / DI registration)
services.AddScoped<IExtractor<Employee>>(_ =>
    new EmployeeCsvExtractor("data/employees_1m.csv"));

services.AddScoped<ILoader<Employee>, SqlEmployeeLoader>();
services.AddSingleton<IEventBus, InMemoryEventBus>();
services.AddScoped<ImportEmployeesUseCase>();
```

### Streaming variant

For large files, swap the use case to `IStreamingExtractor<T>` — the application layer still has no knowledge of CSV files or Conduit internals:

```csharp
// Application/UseCases/StreamEmployeesUseCase.cs
public class StreamEmployeesUseCase(
    IStreamingExtractor<Employee> extractor,
    IEventBus eventBus)
{
    public Task<PipelineResult> ExecuteAsync(CancellationToken ct = default)
        => StreamingEtlPipelineBuilder
            .WithExtractor(extractor)
            .WithName("Stream Employees")
            .WithEventBus(eventBus)
            .WithTransformCallback(e => e)
            .WithLoadCallback((e, _) => SaveAsync(e))
            .RunAsync(ct);

    private Task SaveAsync(Employee e) => Task.CompletedTask; // your persistence call
}
```

### Key benefit

Swapping `CsvExtractor` for an `ApiExtractor`, or `SqlEmployeeLoader` for a `MongoLoader`, requires **zero changes** to your Domain or Application layers. You only update the Infrastructure and composition root.

---

## Running the tests

```bash
dotnet test Conduit.Tests/Conduit.Tests.csproj
```

38 tests covering the event bus, all transformer types, extractors, loaders, the streaming pipeline, and callback handlers.
