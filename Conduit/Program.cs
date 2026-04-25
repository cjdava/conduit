using Conduit.Core.Events;
using Conduit.Core.Interfaces;
using Conduit.EventBus;
using Conduit.Extractors;
using Conduit.Handlers;
using Conduit.Loaders;
using Conduit.Pipeline;
using Conduit.Transformers;

Console.OutputEncoding = System.Text.Encoding.UTF8;

// ─────────────────────────────────────────────────────────────────────────────
// Set up the event bus.
//
// STYLE A — interface-based (full handler class, can be reused across buses)
// STYLE B — callback-based (lambda passed directly, no class needed)
//
// Both styles are shown below. They can be mixed freely.
// ─────────────────────────────────────────────────────────────────────────────
var bus = new InMemoryEventBus();

// Style A: handler class subscribes to four event types at once
var logger = new ConsoleLoggingHandler();
bus.Subscribe((IEventHandler<PipelineStartedEvent>)logger);
bus.Subscribe((IEventHandler<StageCompletedEvent>)logger);
bus.Subscribe((IEventHandler<PipelineCompletedEvent>)logger);
bus.Subscribe((IEventHandler<PipelineFailedEvent>)logger);

// Style B: inline lambda — no class required
bus.Subscribe<PipelineFailedEvent>(e =>
    Console.WriteLine($"  [audit] failure recorded for pipeline {e.PipelineId}"));

// ─────────────────────────────────────────────────────────────────────────────
// Demo 1 — Integer pipeline
// Source   : numbers 1–10
// Transform: keep only even numbers
// Sink     : print to console
// ─────────────────────────────────────────────────────────────────────────────
Console.WriteLine("\n══════════════════════════════════════════");
Console.WriteLine("  Demo 1: Filter even numbers");
Console.WriteLine("══════════════════════════════════════════\n");

var numbersPipeline = EtlPipelineBuilder
    .WithExtractor(new InMemoryExtractor<int>(Enumerable.Range(1, 10), source: "Range(1,10)"))
    .WithName("Even Numbers Pipeline")
    .WithEventBus(bus)
    .WithTransformer(new FilterTransformer<int>(n => n % 2 == 0))
    .WithLoader(new ConsoleLoader<int>());

var numbersResult = await numbersPipeline.RunAsync();
Console.WriteLine($"\nResult: {numbersResult}\n");

// ─────────────────────────────────────────────────────────────────────────────
// Demo 2 — CSV employee pipeline
// Source   : employees.csv
// Transform: chain two filters — Engineering dept + salary > 90 000
//            followed by a string-formatting map
// Sink     : collect in memory, then print a summary
// ─────────────────────────────────────────────────────────────────────────────
Console.WriteLine("══════════════════════════════════════════");
Console.WriteLine("  Demo 2: Senior engineers from CSV");
Console.WriteLine("══════════════════════════════════════════\n");

var csvPath = Path.Combine(AppContext.BaseDirectory, "data", "employees.csv");
var inMemoryLoader = new InMemoryLoader<string>();

var engineeringFilter = new FilterTransformer<Dictionary<string, string>>(r =>
    r.GetValueOrDefault("Department", "") == "Engineering");

var salaryFilter = new FilterTransformer<Dictionary<string, string>>(r =>
    decimal.TryParse(r.GetValueOrDefault("Salary", "0"), out var s) && s > 90_000);

var compositeFilter = new CompositeTransformer<Dictionary<string, string>>(
    [engineeringFilter, salaryFilter]);

var formatStep = new DelegateTransformer<Dictionary<string, string>, string>(
    r => $"{r["Name"]} | {r["Department"]} | ${decimal.Parse(r["Salary"]):N0}");

var csvPipeline = EtlPipelineBuilder
    .WithExtractor(new CsvExtractor(csvPath))
    .WithName("Senior Engineers Pipeline")
    .WithEventBus(bus)
    .WithTransformer(new SequentialTransformer<Dictionary<string, string>, Dictionary<string, string>, string>(compositeFilter, formatStep))
    .WithLoader(inMemoryLoader);

var csvResult = await csvPipeline.RunAsync();
Console.WriteLine($"\nResult: {csvResult}");
Console.WriteLine("\nLoaded records:");
foreach (var record in inMemoryLoader.LoadedRecords)
    Console.WriteLine($"  → {record}");

// ─────────────────────────────────────────────────────────────────────────────
// Demo 3 — Failure handling
// ─────────────────────────────────────────────────────────────────────────────
Console.WriteLine("\n══════════════════════════════════════════");
Console.WriteLine("  Demo 3: Failure handling");
Console.WriteLine("══════════════════════════════════════════\n");

var failingPipeline = EtlPipelineBuilder
    .WithExtractor(new InMemoryExtractor<int>([1, 2, 3], "Test"))
    .WithName("Failing Pipeline")
    .WithEventBus(bus)
    .WithTransformer(new DelegateTransformer<int, int>(n =>
    {
        if (n == 2) throw new InvalidOperationException("Cannot process the value 2!");
        return n * 10;
    }))
    .WithLoader(new ConsoleLoader<int>());

var failResult = await failingPipeline.RunAsync();
Console.WriteLine($"\nResult: {failResult}");

// ─────────────────────────────────────────────────────────────────────────────
// Demo 4 — Streaming pipeline with per-row callbacks
//
// Instead of batch processing, every extracted row individually passes through:
//   transformCallback(row) → loadCallback(transformedRow)
//
// A RowProcessedEvent fires after each row, so observers can react in real time.
// ─────────────────────────────────────────────────────────────────────────────
Console.WriteLine("\n══════════════════════════════════════════");
Console.WriteLine("  Demo 4: Streaming pipeline with callbacks");
Console.WriteLine("══════════════════════════════════════════\n");

// Subscribe a lambda that fires for every individual row
bus.Subscribe<RowProcessedEvent<string>>(e =>
    Console.WriteLine($"  [row #{e.RowIndex}] processed → {e.Row}"));

var streamingPipeline = StreamingEtlPipelineBuilder
    .WithExtractor(new InMemoryExtractor<int>(Enumerable.Range(1, 5), source: "Range(1,5)"))
    .WithName("Streaming Demo")
    .WithEventBus(bus)
    // transform callback — called for each row individually
    .WithTransformCallback(n => $"processed-{n * 10}")
    // load callback — called immediately after each row is transformed
    .WithLoadCallback((string s) => Console.WriteLine($"  [load ] {s}"));

var streamingResult = await streamingPipeline.RunAsync();
Console.WriteLine($"\nResult: {streamingResult}");

