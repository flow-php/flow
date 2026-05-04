# Contracts

Flow Telemetry is designed as a **contract library** - it defines interfaces that allow you to extend and customize
every part of the telemetry pipeline. This page documents all contracts (interfaces) and their available
implementations.

- [⬅️️ Back](/documentation/components/libs/telemetry.md)

[TOC]

## Core Contracts

### Transport

**Interface:** `Flow\Telemetry\Transport\Transport`

Sends serialized telemetry data to backends over the network. Has signal-specific methods: `sendSpans()`,
`sendMetrics()`, `sendLogs()`.

| Implementation  | Package                          | Description                              |
|-----------------|----------------------------------|------------------------------------------|
| `VoidTransport` | `flow-php/telemetry`             | No-op transport that discards all data   |
| `GrpcTransport` | `flow-php/telemetry-otlp-bridge` | gRPC transport (requires ext-grpc)       |
| `CurlTransport` | `flow-php/telemetry-otlp-bridge` | Async curl transport (requires ext-curl) |

---

### ContextStorage

**Interface:** `Flow\Telemetry\Context\ContextStorage`

Stores and retrieves telemetry context (active span, baggage) within a request lifecycle. Enables automatic context
propagation to child spans.

| Implementation         | Package              | Description                                |
|------------------------|----------------------|--------------------------------------------|
| `MemoryContextStorage` | `flow-php/telemetry` | In-memory storage for single-threaded apps |

---

### Serializer

**Interface:** `Flow\Telemetry\Serializer\Serializer`

Converts telemetry data structures (spans, metrics, logs) into wire formats for transmission over transports.

| Implementation       | Package                          | Description                              |
|----------------------|----------------------------------|------------------------------------------|
| `JsonSerializer`     | `flow-php/telemetry-otlp-bridge` | OTLP JSON format                         |
| `ProtobufSerializer` | `flow-php/telemetry-otlp-bridge` | OTLP Protobuf format (requires ext-grpc) |

---

## Tracer Contracts

### SpanProcessor

**Interface:** `Flow\Telemetry\Tracer\SpanProcessor`

Receives span lifecycle events (`onStart`, `onEnd`) and determines how spans are buffered and exported. The processor
sits between the Tracer and SpanExporter.

| Implementation             | Package              | Description                                       |
|----------------------------|----------------------|---------------------------------------------------|
| `PassThroughSpanProcessor` | `flow-php/telemetry` | Exports each span immediately when it ends        |
| `BatchingSpanProcessor`    | `flow-php/telemetry` | Buffers spans and exports in configurable batches |
| `CompositeSpanProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                  |
| `MemorySpanProcessor`      | `flow-php/telemetry` | Stores spans in memory for testing                |
| `VoidSpanProcessor`        | `flow-php/telemetry` | No-op processor that discards all spans           |

---

### SpanExporter

**Interface:** `Flow\Telemetry\Tracer\SpanExporter`

Exports completed spans to external observability backends. Receives batches of spans from processors.

| Implementation        | Package                          | Description                           |
|-----------------------|----------------------------------|---------------------------------------|
| `ConsoleSpanExporter` | `flow-php/telemetry`             | Prints spans to console (development) |
| `MemorySpanExporter`  | `flow-php/telemetry`             | Stores spans in memory (testing)      |
| `VoidSpanExporter`    | `flow-php/telemetry`             | Discards all spans (disabled tracing) |
| `OTLPSpanExporter`    | `flow-php/telemetry-otlp-bridge` | Exports to OTLP-compatible backends   |

---

### Sampler

**Interface:** `Flow\Telemetry\Tracer\Sampler\Sampler`

Makes sampling decisions for traces. Determines whether a span should be recorded and exported based on configurable
rules.

| Implementation             | Package              | Description                                         |
|----------------------------|----------------------|-----------------------------------------------------|
| `AlwaysOnSampler`          | `flow-php/telemetry` | Records all traces                                  |
| `AlwaysOffSampler`         | `flow-php/telemetry` | Records no traces                                   |
| `TraceIdRatioBasedSampler` | `flow-php/telemetry` | Records a configurable percentage of traces         |
| `ParentBasedSampler`       | `flow-php/telemetry` | Inherits sampling decision from parent span context |

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

Processes metric measurements from instruments. Determines how metrics are aggregated and when they are exported.

| Implementation               | Package              | Description                                         |
|------------------------------|----------------------|-----------------------------------------------------|
| `PassThroughMetricProcessor` | `flow-php/telemetry` | Exports each metric immediately when recorded       |
| `BatchingMetricProcessor`    | `flow-php/telemetry` | Buffers metrics and exports in configurable batches |
| `CompositeMetricProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                    |
| `MemoryMetricProcessor`      | `flow-php/telemetry` | Stores metrics in memory for testing                |
| `VoidMetricProcessor`        | `flow-php/telemetry` | No-op processor that discards all metrics           |

---

### MetricExporter

**Interface:** `Flow\Telemetry\Meter\MetricExporter`

Exports collected metrics to external observability backends. Receives batches of metrics from processors.

| Implementation          | Package                          | Description                             |
|-------------------------|----------------------------------|-----------------------------------------|
| `ConsoleMetricExporter` | `flow-php/telemetry`             | Prints metrics to console (development) |
| `MemoryMetricExporter`  | `flow-php/telemetry`             | Stores metrics in memory (testing)      |
| `VoidMetricExporter`    | `flow-php/telemetry`             | Discards all metrics (disabled metrics) |
| `OTLPMetricExporter`    | `flow-php/telemetry-otlp-bridge` | Exports to OTLP-compatible backends     |

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

Processes log records from loggers. Determines how logs are buffered and when they are exported.

| Implementation            | Package              | Description                                      |
|---------------------------|----------------------|--------------------------------------------------|
| `PassThroughLogProcessor` | `flow-php/telemetry` | Exports each log immediately when recorded       |
| `BatchingLogProcessor`    | `flow-php/telemetry` | Buffers logs and exports in configurable batches |
| `CompositeLogProcessor`   | `flow-php/telemetry` | Delegates to multiple processors                 |
| `MemoryLogProcessor`      | `flow-php/telemetry` | Stores logs in memory for testing                |
| `VoidLogProcessor`        | `flow-php/telemetry` | No-op processor that discards all logs           |

---

### LogExporter

**Interface:** `Flow\Telemetry\Logger\LogExporter`

Exports log records to external observability backends. Receives batches of logs from processors.

| Implementation       | Package                          | Description                          |
|----------------------|----------------------------------|--------------------------------------|
| `ConsoleLogExporter` | `flow-php/telemetry`             | Prints logs to console (development) |
| `MemoryLogExporter`  | `flow-php/telemetry`             | Stores logs in memory (testing)      |
| `VoidLogExporter`    | `flow-php/telemetry`             | Discards all logs (disabled logging) |
| `OTLPLogExporter`    | `flow-php/telemetry-otlp-bridge` | Exports to OTLP-compatible backends  |

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

All contracts are designed for extension. To implement a custom exporter, processor, or transport:

1. Implement the relevant interface
2. Pass your implementation to the appropriate provider or processor

```php
<?php

use Flow\Telemetry\Tracer\SpanExporter;
use Flow\Telemetry\Tracer\ReadableSpan;

final class MyCustomSpanExporter implements SpanExporter
{
    /**
     * @param array<ReadableSpan> $spans
     */
    public function export(array $spans): bool
    {
        foreach ($spans as $span) {
            // Send to your custom backend
        }
        return true;
    }
}

// Use with any processor
$processor = pass_through_span_processor(new MyCustomSpanExporter());
```
