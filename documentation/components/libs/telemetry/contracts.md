# Contracts

Flow Telemetry is designed as a **contract library** - it defines interfaces that allow you to extend and customize
every part of the telemetry pipeline. This page documents all contracts (interfaces) and their available
implementations.

[DOC_LINK:/documentation/components/libs/telemetry.md]

[TOC]

## Core Contracts

### Transport

**Interface:** `Flow\Telemetry\Transport\Transport`

Sends serialized telemetry data to backends over the network. The signal type is inferred from the batch payload type
(`LogsBatch`, `MetricsBatch`, `TracesBatch`). Single method:

```php ignore
public function send(LogsBatch|MetricsBatch|TracesBatch $batch) : void;
public function shutdown() : void;
```

| Implementation  | Package                          | Description                              |
|-----------------|----------------------------------|------------------------------------------|
| `VoidTransport` | `flow-php/telemetry`             | No-op transport that discards all data   |
| `GrpcTransport` | `flow-php/telemetry-otlp-bridge` | gRPC transport (requires ext-grpc)       |
| `CurlTransport` | `flow-php/telemetry-otlp-bridge` | Async curl transport (requires ext-curl) |

---

### Exporter

**Interface:** `Flow\Telemetry\Exporter\Exporter`

Exports telemetry batches to external observability backends. A single exporter handles all three signals via
`match(true)` instanceof dispatch on `LogsBatch | MetricsBatch | TracesBatch`.

```php ignore
public function export(LogsBatch|MetricsBatch|TracesBatch $batch) : bool;

/** @return array<Transport> */
public function transports() : array;
```

| Implementation    | Package                          | Description                                       |
|-------------------|----------------------------------|---------------------------------------------------|
| `VoidExporter`    | `flow-php/telemetry`             | Discards all data (disabled telemetry)            |
| `MemoryExporter`  | `flow-php/telemetry`             | Stores logs/metrics/spans in memory (testing)     |
| `ConsoleExporter` | `flow-php/telemetry`             | Renders logs/metrics/spans to console (debugging) |
| `OTLPExporter`    | `flow-php/telemetry-otlp-bridge` | Sends batches over the configured Transport       |

---

### Batch value objects

Three small immutable value objects in `Flow\Telemetry\Batch` carry the payload across Transport / Exporter boundaries:

- `LogsBatch(array<LogEntry> $entries)`
- `MetricsBatch(array<Metric> $metrics)`
- `TracesBatch(array<Span> $spans)`

---

### ContextStorage

**Interface:** `Flow\Telemetry\Context\ContextStorage`

Stores and retrieves telemetry context (active span, baggage) within a request lifecycle. `attach()` returns a
`Scope` that must be detached in reverse order; a span becomes the parent of later spans only while its scope is
attached - see `Tracer::activate()`.

| Implementation         | Package              | Description                                |
|------------------------|----------------------|--------------------------------------------|
| `MemoryContextStorage` | `flow-php/telemetry` | In-memory storage for single-threaded apps |

---

## Tracer Contracts

### SpanProcessor

**Interface:** `Flow\Telemetry\Tracer\SpanProcessor`

Receives span lifecycle events (`onStart`, `onEnd`) and determines how spans are buffered and exported. The processor
sits between the Tracer and the unified `Exporter`. The `exporter()` accessor returns the unified `Exporter`.

| Implementation             | Package              | Description                                       |
|----------------------------|----------------------|---------------------------------------------------|
| `PassThroughSpanProcessor` | `flow-php/telemetry` | Exports each span immediately when it ends        |
| `BatchingSpanProcessor`    | `flow-php/telemetry` | Buffers spans and exports in configurable batches |
| `CompositeSpanProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                  |
| `MemorySpanProcessor`      | `flow-php/telemetry` | Stores spans in memory for testing                |
| `VoidSpanProcessor`        | `flow-php/telemetry` | No-op processor that discards all spans           |

---

### Sampler

**Interface:** `Flow\Telemetry\Tracer\Sampler\Sampler`

Makes sampling decisions for traces. Determines whether a span should be recorded and exported based on configurable
rules.

| Implementation             | Package              | Description                                                                                              |
|----------------------------|----------------------|----------------------------------------------------------------------------------------------------------|
| `AlwaysOnSampler`          | `flow-php/telemetry` | Records all traces                                                                                       |
| `AlwaysOffSampler`         | `flow-php/telemetry` | Records no traces                                                                                        |
| `TraceIdRatioBasedSampler` | `flow-php/telemetry` | Records a configurable percentage of traces                                                              |
| `ParentBasedSampler`       | `flow-php/telemetry` | Inherits sampling decision from parent span context                                                      |
| `AttributeMatchingSampler` | `flow-php/telemetry` | Drops spans matching an `AttributeFilter` (start-time attributes), defers the rest to a delegate sampler |

---

### SpanEvent

**Interface:** `Flow\Telemetry\Tracer\SpanEvent`

Represents timestamped events that occurred during a span's lifetime (e.g., exceptions, state changes).

| Implementation | Package              | Description                           |
|----------------|----------------------|---------------------------------------|
| `GenericEvent` | `flow-php/telemetry` | General-purpose event with attributes |

---

## Meter Contracts

### MetricProcessor

**Interface:** `Flow\Telemetry\Meter\MetricProcessor`

Processes metric measurements from instruments. Determines how metrics are aggregated and when they are exported. The
`exporter()` accessor returns the unified `Exporter`.

| Implementation               | Package              | Description                                         |
|------------------------------|----------------------|-----------------------------------------------------|
| `PassThroughMetricProcessor` | `flow-php/telemetry` | Exports each metric immediately when recorded       |
| `BatchingMetricProcessor`    | `flow-php/telemetry` | Buffers metrics and exports in configurable batches |
| `CompositeMetricProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                    |
| `MemoryMetricProcessor`      | `flow-php/telemetry` | Stores metrics in memory for testing                |
| `VoidMetricProcessor`        | `flow-php/telemetry` | No-op processor that discards all metrics           |

---

### Instrument

**Interface:** `Flow\Telemetry\Meter\Instrument\Instrument`

Base interface for metric instruments. Instruments are the API through which measurements are recorded.

| Implementation  | Package              | Description                                       |
|-----------------|----------------------|---------------------------------------------------|
| `Counter`       | `flow-php/telemetry` | Monotonically increasing value (e.g., requests)   |
| `UpDownCounter` | `flow-php/telemetry` | Value that can increase or decrease (e.g., queue) |
| `Gauge`         | `flow-php/telemetry` | Point-in-time measurement (e.g., CPU usage)       |
| `Histogram`     | `flow-php/telemetry` | Distribution of values (e.g., latency)            |

---

## Logger Contracts

### LogProcessor

**Interface:** `Flow\Telemetry\Logger\LogProcessor`

Processes log records from loggers. Determines how logs are buffered and when they are exported. A `LogSink`
(marker interface extending `LogProcessor`) identifies the terminal/leaf processors; `PipelineLogProcessor` runs an
ordered chain of `LogMiddleware` and forwards survivors to one `LogSink`.

| Implementation            | Package              | Sink? | Description                                            |
|---------------------------|----------------------|-------|--------------------------------------------------------|
| `PassThroughLogProcessor` | `flow-php/telemetry` | yes   | Exports each log immediately when recorded             |
| `BatchingLogProcessor`    | `flow-php/telemetry` | yes   | Buffers logs and exports in configurable batches       |
| `CompositeLogProcessor`   | `flow-php/telemetry` | yes   | Delegates to multiple processors                       |
| `MemoryLogProcessor`      | `flow-php/telemetry` | yes   | Stores logs in memory for testing                      |
| `VoidLogProcessor`        | `flow-php/telemetry` | yes   | No-op processor that discards all logs                 |
| `PipelineLogProcessor`    | `flow-php/telemetry` | no    | Runs middleware in order, then forwards to a `LogSink` |

### LogMiddleware

**Interface:** `Flow\Telemetry\Logger\LogMiddleware`

A chainable step inside a `PipelineLogProcessor`. `process(LogEntry): ?LogEntry` returns the entry to pass on (possibly
enriched) or `null` to drop it. Stateless - flush/shutdown belong to the pipeline's `LogSink`.

| Implementation                    | Package              | Description                                          |
|-----------------------------------|----------------------|------------------------------------------------------|
| `EnrichingLogMiddleware`          | `flow-php/telemetry` | Merges default attributes (call-site values win)     |
| `SeverityFilteringLogMiddleware`  | `flow-php/telemetry` | Drops log entries below a minimum severity threshold |
| `AttributeFilteringLogMiddleware` | `flow-php/telemetry` | Drops/keeps log entries by an `AttributeFilter`      |

---

## Propagation Contracts

### TextMapPropagator

**Interface:** `Flow\Telemetry\Propagation\TextMapPropagator`

Extracts and injects trace context across process boundaries using text-based carriers (HTTP headers, message metadata).

| Implementation        | Package              | Description                                          |
|-----------------------|----------------------|------------------------------------------------------|
| `W3CTraceContext`     | `flow-php/telemetry` | W3C Trace Context standard (traceparent, tracestate) |
| `CompositePropagator` | `flow-php/telemetry` | Delegates to multiple propagators                    |

---

### BaggagePropagator

**Interface:** `Flow\Telemetry\Propagation\BaggagePropagator`

Propagates application-specific key-value pairs (baggage) across process boundaries alongside trace context.

| Implementation | Package              | Description          |
|----------------|----------------------|----------------------|
| `W3CBaggage`   | `flow-php/telemetry` | W3C Baggage standard |

---

### Carrier

**Interface:** `Flow\Telemetry\Propagation\Carrier`

Abstraction over the transport mechanism for context propagation. Provides get/set operations for header-like values.

| Implementation | Package              | Description                              |
|----------------|----------------------|------------------------------------------|
| `ArrayCarrier` | `flow-php/telemetry` | Array-backed carrier for in-memory usage |

---

## Implementing Custom Contracts

All contracts are designed for extension. To implement a custom exporter:

```php
<?php

use Flow\Telemetry\Batch\{LogsBatch, MetricsBatch, TracesBatch};
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Transport\{Transport, VoidTransport};

final class MyCustomExporter implements Exporter
{
    public function export(LogsBatch|MetricsBatch|TracesBatch $batch) : bool
    {
        match (true) {
            $batch instanceof TracesBatch => $this->writeSpans($batch->spans),
            $batch instanceof MetricsBatch => $this->writeMetrics($batch->metrics),
            $batch instanceof LogsBatch => $this->writeLogs($batch->entries),
        };

        return true;
    }

    public function transports() : array
    {
        return [new VoidTransport()];
    }

    // ... per-signal helpers
}

$processor = pass_through_span_processor(new MyCustomExporter());
```
