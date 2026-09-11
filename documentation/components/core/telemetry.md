# Telemetry

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Introduction

Telemetry integration in Flow PHP provides observability into your ETL pipelines through distributed tracing, metrics
collection, and structured logging. Understanding what happens inside your data processing workflows is essential for:

- **Debugging** - Identify bottlenecks and failures in complex pipelines
- **Monitoring** - Track performance metrics in production environments
- **Optimization** - Measure throughput and resource usage to improve efficiency
- **Compliance** - Maintain audit trails of data processing operations

Flow PHP's telemetry is built on OpenTelemetry-compatible concepts, allowing integration with popular observability
platforms like Jaeger, Grafana, Datadog, and others.

## Quick Start

The simplest way to get started with telemetry is using console exporters for local development:

```php
<?php

use Flow\Clock\SystemClock;
use function Flow\ETL\DSL\{config_builder, df, from_array, telemetry_options, to_output};
use function Flow\Telemetry\DSL\{
    batching_log_processor, batching_metric_processor, batching_span_processor,
    console_exporter,
    logger_provider, memory_context_storage, meter_provider, resource, telemetry, tracer_provider
};

$clock = new SystemClock(new DateTimeZone('UTC'));
$contextStorage = memory_context_storage();

$telemetry = telemetry(
    resource(['service.name' => 'my-etl-pipeline']),
    tracer_provider(batching_span_processor(console_exporter()), $clock, $contextStorage),
    meter_provider(batching_metric_processor(console_exporter()), $clock),
    logger_provider(batching_log_processor(console_exporter()), $clock, $contextStorage),
);
$telemetry->registerShutdownFunction();

df(config_builder()->withTelemetry($telemetry, telemetry_options(
    trace_loading: true,
    trace_transformations: true,
    collect_metrics: true,
)))
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice'],
        ['id' => 2, 'name' => 'Bob'],
    ]))
    ->write(to_output())
    ->run();
```

## Telemetry Options

Flow PHP provides granular control over what telemetry data is collected through `telemetry_options()`:

| Option                  | Description                                  | Performance Impact                      |
|-------------------------|----------------------------------------------|-----------------------------------------|
| `trace_loading`         | Creates spans for each loader execution      | Low                                     |
| `trace_transformations` | Creates spans for each transformer execution | Medium (many transformers = many spans) |
| `collect_metrics`       | Records counters and throughput metrics      | Low                                     |

### Selective Configuration

```php ignore
// Only trace loaders (useful for debugging slow writes)
telemetry_options(trace_loading: true)

// Only collect metrics (minimal overhead)
telemetry_options(collect_metrics: true)

// Full observability
telemetry_options(
    trace_loading: true,
    trace_transformations: true,
    collect_metrics: true,
)
```

You can also use the fluent builder pattern:

```php
telemetry_options()
    ->traceLoading(true)
    ->traceTransformations(true)
    ->collectMetrics(true);
```

## Configuration

### Basic Configuration

Enable telemetry by passing a `Telemetry` instance to the config builder:

```php
use function Flow\ETL\DSL\{config_builder, telemetry_options};

$config = config_builder()
    ->withTelemetry($telemetry, telemetry_options(
        trace_loading: true,
        collect_metrics: true,
    ))
    ->build();
```

### Console Exporters (Development)

Console exporters output telemetry data directly to stdout with ASCII table formatting. They're ideal for local
development and debugging:

```php
use function Flow\Telemetry\DSL\console_exporter;

// Spans are displayed as tables with trace IDs, durations, and attributes
$spanExporter = console_exporter(colors: true);

// Metrics show counters and throughput values
$metricExporter = console_exporter(colors: true);

// Logs are formatted with severity-based coloring
$logExporter = console_exporter(colors: true, maxBodyLength: 100);
```

### OTLP Export (Production)

For production environments, send telemetry to an OTLP-compatible collector using the OTLP bridge:

```php
use Flow\Clock\SystemClock;
use function Flow\ETL\DSL\{config_builder, telemetry_options};
use function Flow\Telemetry\DSL\{
    batching_log_processor, batching_metric_processor, batching_span_processor,
    logger_provider, memory_context_storage, meter_provider, resource, telemetry, tracer_provider
};
use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport, otlp_json_serializer,
    otlp_exporter, otlp_exporter, otlp_exporter
};

$clock = new SystemClock(new DateTimeZone('UTC'));
$contextStorage = memory_context_storage();

// Configure OTLP transport
$transport = otlp_curl_transport('http://localhost:4318', otlp_json_serializer());

$telemetry = telemetry(
    resource([
        'service.name' => 'my-etl-pipeline',
        'service.version' => '1.0.0',
        'service.namespace' => 'my-company',
    ]),
    tracer_provider(batching_span_processor(otlp_exporter($transport)), $clock, $contextStorage),
    meter_provider(batching_metric_processor(otlp_exporter($transport)), $clock),
    logger_provider(batching_log_processor(otlp_exporter($transport)), $clock, $contextStorage),
);
$telemetry->registerShutdownFunction();
```

### Transport Options

#### Curl Transport (Async)

Recommended for better performance - uses curl_multi for non-blocking I/O:

```php
use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_curl_options, otlp_json_serializer};

$transport = otlp_curl_transport(
    endpoint: 'http://localhost:4318',
    serializer: otlp_json_serializer(),
    options: otlp_curl_options()
        ->withTimeout(60)
        ->withConnectTimeout(15)
        ->withHeader('Authorization', 'Bearer token')
        ->withCompression(),
);
```

#### gRPC Transport

For gRPC endpoints (requires ext-grpc). OTLP/gRPC mandates Protobuf, so the transport builds the request factory
internally - no serializer parameter.

```php
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_grpc_transport;

$transport = otlp_grpc_transport(endpoint: 'localhost:4317');
```

## Collected Data

### Spans

Every DataFrame execution creates a root span with the following attributes:

| Attribute                             | Description                                   |
|---------------------------------------|-----------------------------------------------|
| `flow.etl.dataframe.id`               | Unique identifier for the DataFrame execution |
| `flow.etl.dataframe.name`             | Configured DataFrame name                     |
| `flow.etl.rows.total`                 | Total number of rows processed                |
| `flow.etl.rows.throughput.per_second` | Processing throughput                         |
| `flow.etl.memory.min`                 | Minimum memory consumption during execution (MB) |
| `flow.etl.memory.max`                 | Maximum memory consumption during execution (MB) |

When `trace_loading` is enabled, child spans are created for each loader with:

- Span name: Loader class name (e.g., `Flow\ETL\Loader\StreamLoader`)
- Parent: DataFrame span
- Status: OK on success, ERROR on failure

When `trace_transformations` is enabled, child spans are created for each transformer with:

- Span name: Transformer class name (e.g., `Flow\ETL\Transformer\EntryNameTransformer`)
- Parent: DataFrame span
- Status: OK on success, ERROR on failure

### Metrics

When `collect_metrics` is enabled:

| Metric                     | Type       | Unit       | Description                        |
|----------------------------|------------|------------|------------------------------------|
| `flow.etl.rows.processed`  | Counter    | `{row}`    | Cumulative count of processed rows |
| `flow.etl.rows.throughput` | Throughput | `{row}/s`  | Rows processed per second          |

### Logs

Structured logs are emitted at DEBUG level for pipeline events:

- **Pipeline start** - Logged with configuration details (cache type, serializer, optimizers, spill storages)
- **Pipeline completion** - Logged with summary statistics (total rows, memory usage)
- **Errors** - Logged at ERROR level with exception details

## Processing and Batching

Telemetry data is processed through configurable processors:

### Batching Processors (Recommended)

Collect telemetry in memory and export in batches for better performance:

```php ignore
use function Flow\Telemetry\DSL\{
    batching_span_processor, batching_metric_processor, batching_log_processor
};

// Export spans in batches of 512 (default)
batching_span_processor($exporter, batchSize: 512)

// Export metrics in batches of 512 (default)
batching_metric_processor($exporter, batchSize: 512)

// Export logs in batches of 512 (default)
batching_log_processor($exporter, batchSize: 512)
```

### Pass-Through Processors (Debugging)

Export immediately for real-time visibility during debugging:

```php ignore
use function Flow\Telemetry\DSL\{
    pass_through_span_processor, pass_through_metric_processor, pass_through_log_processor
};

pass_through_span_processor($exporter)
pass_through_metric_processor($exporter)
pass_through_log_processor($exporter)
```

## DSL Functions Reference

### Core Telemetry

| Function              | Description                       |
|-----------------------|-----------------------------------|
| `telemetry()`         | Create a Telemetry instance       |
| `resource()`          | Create a Resource with attributes |
| `telemetry_options()` | Configure telemetry options       |

### Providers

| Function            | Description             |
|---------------------|-------------------------|
| `tracer_provider()` | Create a TracerProvider |
| `meter_provider()`  | Create a MeterProvider  |
| `logger_provider()` | Create a LoggerProvider |

### Processors

| Function                               | Description                               |
|----------------------------------------|-------------------------------------------|
| `batching_span_processor()`            | Batch span processing                     |
| `batching_metric_processor()`          | Batch metric processing                   |
| `batching_log_processor()`             | Batch log processing                      |
| `pass_through_span_processor()`        | Immediate span export                     |
| `pass_through_metric_processor()`      | Immediate metric export                   |
| `pass_through_log_processor()`         | Immediate log export                      |
| `pipeline_log_processor()`             | Run logs through middleware, then a sink  |
| `enriching_log_middleware()`           | Merge default attributes into log records |
| `severity_filtering_log_middleware()`  | Filter logs by severity                   |
| `attribute_filtering_log_middleware()` | Filter logs by attribute matcher          |

### Samplers

| Function                         | Description                                                           |
|----------------------------------|-----------------------------------------------------------------------|
| `always_on_sampler()`            | Record and sample every span                                          |
| `always_off_sampler()`           | Drop every span                                                       |
| `trace_id_ratio_based_sampler()` | Sample a deterministic fraction of traces                             |
| `parent_based_sampler()`         | Honor the parent's decision; root sampler for parentless spans        |
| `attribute_matching_sampler()`   | Drop spans matching an attribute filter, defer the rest to a delegate |

### Exporters (Console)

| Function             | Description               |
|----------------------|---------------------------|
| `console_exporter()` | Export spans to console   |
| `console_exporter()` | Export metrics to console |
| `console_exporter()` | Export logs to console    |

### Exporters (OTLP)

| Function          | Description             |
|-------------------|-------------------------|
| `otlp_exporter()` | Export spans via OTLP   |
| `otlp_exporter()` | Export metrics via OTLP |
| `otlp_exporter()` | Export logs via OTLP    |

### Transport (OTLP)

| Function                     | Description            |
|------------------------------|------------------------|
| `otlp_curl_transport()`      | Async curl transport   |
| `otlp_grpc_transport()`      | gRPC transport         |
| `otlp_json_serializer()`     | JSON serialization     |
| `otlp_protobuf_serializer()` | Protobuf serialization |

### Testing

| Function                  | Description             |
|---------------------------|-------------------------|
| `memory_exporter()`       | Store spans in memory   |
| `memory_exporter()`       | Store metrics in memory |
| `memory_exporter()`       | Store logs in memory    |
| `void_span_processor()`   | No-op span processor    |
| `void_metric_processor()` | No-op metric processor  |
| `void_log_processor()`    | No-op log processor     |

## Best Practices

### When to Enable Each Option

- **`trace_loading: true`** - Enable when debugging write operations or when you need to identify slow loaders
- **`trace_transformations: true`** - Enable temporarily when debugging transformation logic; disable in production for
  high-throughput pipelines
- **`collect_metrics: true`** - Safe to enable in production; provides valuable throughput data with minimal overhead

### Performance Considerations

1. **Use batching processors** - Batching reduces I/O overhead significantly compared to immediate export
2. **Disable transformation tracing in production** - Pipelines with many transformations create many spans
3. **Configure appropriate batch sizes** - Larger batches are more efficient but use more memory
4. **Use curl transport** - The async curl transport has better performance than PSR-18 HTTP clients
5. **Call `registerShutdownFunction()`** - Ensures all pending telemetry is flushed on script termination

### Sampling Strategies

For high-volume pipelines, consider sampling to reduce telemetry volume:

```php
use function Flow\Telemetry\DSL\{parent_based_sampler, trace_id_ratio_based_sampler};

// Honor the parent's decision; sample 10% of traces that originate here
tracer_provider($processor, $clock, $contextStorage, parent_based_sampler(trace_id_ratio_based_sampler(0.1)));
```

See the [breaking change note](../libs/telemetry.md#sampling) in the telemetry library docs: the default sampler
changed from `AlwaysOnSampler` to `ParentBasedSampler(AlwaysOnSampler)`, so spans whose parent is unsampled are now
dropped.

### Resource Attributes

Include meaningful resource attributes to identify your service:

```php
resource([
    'service.name' => 'order-processor',
    'service.version' => '2.1.0',
    'service.namespace' => 'ecommerce',
    'deployment.environment' => 'production',
]);
```

## Disabling Telemetry

Telemetry is disabled by default. If you've enabled it but want to disable it for certain environments, simply don't
pass telemetry configuration:

```php
// No telemetry - uses void providers internally
$config = config_builder()->build();
```

Or use void providers explicitly:

```php
use function Flow\Telemetry\DSL\{void_span_processor, void_metric_processor, void_log_processor};

$telemetry = telemetry(
    resource(['service.name' => 'my-service']),
    tracer_provider(void_span_processor(), $clock, $contextStorage),
    meter_provider(void_metric_processor(), $clock),
    logger_provider(void_log_processor(), $clock, $contextStorage),
);
```
