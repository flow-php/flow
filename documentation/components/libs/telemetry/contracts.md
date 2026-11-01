# Contracts

Flow Telemetry is a contract library. Its interfaces let you extend and change each part of the telemetry pipeline.
This page lists all contracts (interfaces) and their available implementations.

[DOC_LINK:/documentation/components/libs/telemetry.md]

[TOC]

## Core Contracts

### Transport

Interface: `Flow\Bridge\Telemetry\OTLP\Transport\Transport`

Sends telemetry to an OTLP backend. The signal type is `Signals::$type`:

```php ignore
public function send(Signals $signal) : void;
public function shutdown() : void;
```

| Implementation       | Package                          | Description                                          |
|----------------------|----------------------------------|------------------------------------------------------|
| `GrpcTransport`      | `flow-php/telemetry-otlp-bridge` | Non-blocking gRPC transport (requires ext-grpc)      |
| `CurlTransport`      | `flow-php/telemetry-otlp-bridge` | Synchronous HTTP transport (requires ext-curl)       |
| `AsyncCurlTransport` | `flow-php/telemetry-otlp-bridge` | Non-blocking HTTP transport (requires ext-curl)      |
| `StreamTransport`    | `flow-php/telemetry-otlp-bridge` | Writes JSONL to a file path or a `php://` stream     |

---

### Exporter

Interface: `Flow\Telemetry\Exporter\Exporter`

Exports telemetry to external observability backends. One exporter handles all three signals with a `match` on
`Signals::$type`.

```php ignore
public function export(Signals $signal) : bool;
public function shutdown() : void;
```

| Implementation    | Package                          | Description                                       |
|-------------------|----------------------------------|---------------------------------------------------|
| `VoidExporter`    | `flow-php/telemetry`             | Discards all data (disabled telemetry)            |
| `MemoryExporter`  | `flow-php/telemetry`             | Stores logs/metrics/spans in memory (testing)     |
| `ConsoleExporter` | `flow-php/telemetry`             | Renders logs/metrics/spans to console (debugging) |
| `OTLPExporter`    | `flow-php/telemetry-otlp-bridge` | Sends batches over the configured Transport       |

---

### Signals

`Flow\Telemetry\Signal\Signals` carries the payload of one signal type across Exporter and Transport boundaries:

- `Signals::logs(array<LogEntry>)`, read with `allLogs()`
- `Signals::metrics(array<Metric>)`, read with `allMetrics()`
- `Signals::traces(array<Span>)`, read with `allSpans()`

`$type` is a `SignalType`: `LOGS`, `METRICS` or `TRACES`.

---

### ContextStorage

Interface: `Flow\Telemetry\Context\ContextStorage`

Stores and gets the telemetry context (active span, baggage) in a request lifecycle. `attach()` returns a `Scope`.
You must detach the scopes in reverse order. A span is the parent of later spans only while its scope is attached.
For more information, read `Tracer::activate()`.

| Implementation         | Package              | Description                                |
|------------------------|----------------------|--------------------------------------------|
| `MemoryContextStorage` | `flow-php/telemetry` | In-memory storage for single-threaded apps |

---

## Tracer Contracts

### SpanProcessor

Interface: `Flow\Telemetry\Tracer\SpanProcessor`

Receives span lifecycle events (`onStart`, `onEnd`) and decides how spans are buffered and exported. The processor
is between the Tracer and the unified `Exporter`. The `exporter()` accessor returns the unified `Exporter`.

| Implementation             | Package              | Description                                       |
|----------------------------|----------------------|---------------------------------------------------|
| `PassThroughSpanProcessor` | `flow-php/telemetry` | Exports each span immediately at its end          |
| `BatchingSpanProcessor`    | `flow-php/telemetry` | Buffers spans and exports in configurable batches |
| `CompositeSpanProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                  |
| `MemorySpanProcessor`      | `flow-php/telemetry` | Stores spans in memory for testing                |
| `VoidSpanProcessor`        | `flow-php/telemetry` | No-op processor that discards all spans           |

---

### Sampler

Interface: `Flow\Telemetry\Tracer\Sampler\Sampler`

Makes sampling decisions for traces. It uses configurable rules to decide which spans are recorded and exported.

| Implementation              | Package              | Description                                                                                              |
|-----------------------------|----------------------|----------------------------------------------------------------------------------------------------------|
| `AlwaysOnSampler`           | `flow-php/telemetry` | Records all traces                                                                                       |
| `AlwaysOffSampler`          | `flow-php/telemetry` | Records no traces                                                                                        |
| `TraceIdRatioBasedSampler`  | `flow-php/telemetry` | Records a configurable percentage of traces                                                              |
| `ParentBasedSampler`        | `flow-php/telemetry` | Inherits sampling decision from parent span context                                                      |
| `AttributeFilteringSampler` | `flow-php/telemetry` | Drops spans matching an `AttributeFilter` (start-time attributes), defers the rest to a delegate sampler |

---

### SpanEvent

Interface: `Flow\Telemetry\Tracer\SpanEvent`

Represents timestamped events in the lifetime of a span, for example exceptions or state changes.

| Implementation | Package              | Description                           |
|----------------|----------------------|---------------------------------------|
| `GenericEvent` | `flow-php/telemetry` | General-purpose event with attributes |

---

## Meter Contracts

### MetricProcessor

Interface: `Flow\Telemetry\Meter\MetricProcessor`

Processes metric measurements from instruments. It decides how metrics are aggregated and at what time they are
exported. The `exporter()` accessor returns the unified `Exporter`.

| Implementation               | Package              | Description                                         |
|------------------------------|----------------------|-----------------------------------------------------|
| `PassThroughMetricProcessor` | `flow-php/telemetry` | Exports each metric immediately after its record    |
| `BatchingMetricProcessor`    | `flow-php/telemetry` | Buffers metrics and exports in configurable batches |
| `CompositeMetricProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                    |
| `MemoryMetricProcessor`      | `flow-php/telemetry` | Stores metrics in memory for testing                |
| `VoidMetricProcessor`        | `flow-php/telemetry` | No-op processor that discards all metrics           |

---

### Instrument

Interface: `Flow\Telemetry\Meter\Instrument\Instrument`

Base interface for metric instruments. Instruments are the API that records measurements.

| Implementation  | Package              | Description                                               |
|-----------------|----------------------|-----------------------------------------------------------|
| `Counter`       | `flow-php/telemetry` | Monotonically increasing value (for example requests)     |
| `UpDownCounter` | `flow-php/telemetry` | Value that can increase or decrease (for example a queue) |
| `Gauge`         | `flow-php/telemetry` | Point-in-time measurement (for example CPU usage)         |
| `Histogram`     | `flow-php/telemetry` | Distribution of values (for example latency)              |

---

## Logger Contracts

### LogProcessor

Interface: `Flow\Telemetry\Logger\LogProcessor`

Processes log records from loggers. It decides how logs are buffered and at what time they are exported. A `LogSink`
(a marker interface that extends `LogProcessor`) marks the terminal/leaf processors. `PipelineLogProcessor` runs an
ordered chain of `LogMiddleware` and sends the remaining entries to one `LogSink`.

| Implementation            | Package              | Sink? | Description                                            |
|---------------------------|----------------------|-------|--------------------------------------------------------|
| `PassThroughLogProcessor` | `flow-php/telemetry` | yes   | Exports each log immediately after its record          |
| `BatchingLogProcessor`    | `flow-php/telemetry` | yes   | Buffers logs and exports in configurable batches       |
| `CompositeLogProcessor`   | `flow-php/telemetry` | yes   | Delegates to multiple processors                       |
| `MemoryLogProcessor`      | `flow-php/telemetry` | yes   | Stores logs in memory for testing                      |
| `VoidLogProcessor`        | `flow-php/telemetry` | yes   | No-op processor that discards all logs                 |
| `PipelineLogProcessor`    | `flow-php/telemetry` | no    | Runs middleware in order, then forwards to a `LogSink` |

### LogMiddleware

Interface: `Flow\Telemetry\Logger\LogMiddleware`

A chainable step in a `PipelineLogProcessor`. `process(LogEntry): ?LogEntry` returns the entry for the next step
(possibly enriched), or `null` to drop it. A middleware has no state. Flush and shutdown belong to the `LogSink` of the
pipeline.

| Implementation                    | Package              | Description                                          |
|-----------------------------------|----------------------|------------------------------------------------------|
| `EnrichingLogMiddleware`          | `flow-php/telemetry` | Merges default attributes (call-site values win)     |
| `SeverityFilteringLogMiddleware`  | `flow-php/telemetry` | Drops log entries below a minimum severity threshold |
| `AttributeFilteringLogMiddleware` | `flow-php/telemetry` | Drops/keeps log entries by an `AttributeFilter`      |

---

## Propagation Contracts

### TextMapPropagator

Interface: `Flow\Telemetry\Propagation\TextMapPropagator`

Extracts and injects trace context across process boundaries with text-based carriers (HTTP headers, message metadata).

| Implementation        | Package              | Description                                          |
|-----------------------|----------------------|------------------------------------------------------|
| `W3CTraceContext`     | `flow-php/telemetry` | W3C Trace Context standard (traceparent, tracestate) |
| `CompositePropagator` | `flow-php/telemetry` | Delegates to multiple propagators                    |

---

### BaggagePropagator

Interface: `Flow\Telemetry\Propagation\BaggagePropagator`

Propagates application-specific key-value pairs (baggage) across process boundaries, together with the trace context.

| Implementation | Package              | Description          |
|----------------|----------------------|----------------------|
| `W3CBaggage`   | `flow-php/telemetry` | W3C Baggage standard |

---

### Carrier

Interface: `Flow\Telemetry\Propagation\Carrier`

Abstraction over the transport mechanism for context propagation. It has get/set operations for header-like values.

| Implementation | Package              | Description                              |
|----------------|----------------------|------------------------------------------|
| `ArrayCarrier` | `flow-php/telemetry` | Array-backed carrier for in-memory usage |

---

## Implementing Custom Contracts

You can extend all contracts. To implement a custom exporter:

```php
<?php

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\{Signals, SignalType};

use function Flow\Telemetry\DSL\pass_through_span_processor;

final class MyCustomExporter implements Exporter
{
    public function export(Signals $signal) : bool
    {
        match ($signal->type) {
            SignalType::TRACES => $this->writeSpans($signal->allSpans()),
            SignalType::METRICS => $this->writeMetrics($signal->allMetrics()),
            SignalType::LOGS => $this->writeLogs($signal->allLogs()),
        };

        return true;
    }

    public function shutdown() : void
    {
    }

    // ... per-signal helpers
}

$processor = pass_through_span_processor(new MyCustomExporter());
```
