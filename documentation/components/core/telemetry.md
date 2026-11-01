# Telemetry

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Introduction

Telemetry shows what happens inside your ETL pipelines. Flow PHP gives distributed tracing, metrics and structured
logs. Use them for:

- Debugging: find slow steps and failures in large pipelines
- Monitoring: follow performance metrics in production
- Optimization: measure throughput and resource usage
- Compliance: keep an audit trail of data processing operations

The telemetry of Flow PHP uses OpenTelemetry-compatible concepts. You can send its data to observability platforms
such as Jaeger, Grafana and Datadog.

## Quick Start

For local development, start with console exporters:

```php
<?php

use Flow\ETL\Foundation\Clock\SystemClock;
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

`telemetry_options()` controls which telemetry data Flow PHP collects:

| Option                  | Description                                                             | Performance Impact                      |
|-------------------------|-------------------------------------------------------------------------|-----------------------------------------|
| `trace_loading`         | Creates spans for each loader execution                                 | Low                                     |
| `trace_transformations` | Creates spans for each transformer execution and each processor or join | Medium (many transformers = many spans) |
| `collect_metrics`       | Records counters and throughput metrics                                 | Low                                     |

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

You can also use the fluent builder:

```php
telemetry_options()
    ->traceLoading(true)
    ->traceTransformations(true)
    ->collectMetrics(true);
```

## Configuration

### Basic Configuration

To enable telemetry, give a `Telemetry` instance to the configuration builder:

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

Console exporters write telemetry data to stdout as ASCII tables. Use them for local development and debugging:

```php
use function Flow\Telemetry\DSL\console_exporter;

// Spans are displayed as tables with trace IDs, durations, and attributes
$spanExporter = console_exporter(colors: true);

// Metrics show counters and throughput values
$metricExporter = console_exporter(colors: true);

// Logs are formatted with severity-based coloring
$logExporter = console_exporter(colors: true, maxLogBodyLength: 100);
```

### OTLP Export (Production)

For production, use the OTLP bridge to send telemetry to an OTLP-compatible collector:

```php
use Flow\ETL\Foundation\Clock\SystemClock;
use function Flow\ETL\DSL\{config_builder, telemetry_options};
use function Flow\Telemetry\DSL\{
    batching_log_processor, batching_metric_processor, batching_span_processor,
    logger_provider, memory_context_storage, meter_provider, resource, telemetry, tracer_provider
};
use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_exporter, otlp_json_serializer};

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

This transport is faster. It uses curl_multi for non-blocking I/O:

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

Use it for gRPC endpoints. It needs ext-grpc. OTLP/gRPC must use Protobuf, so the transport builds the request
factory itself. It has no serializer parameter.

```php
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_grpc_transport;

$transport = otlp_grpc_transport(endpoint: 'localhost:4317');
```

## Collected Data

### Spans

Each run of a DataFrame creates a root span with these attributes:

| Attribute                             | Description                                      |
|---------------------------------------|--------------------------------------------------|
| `flow.etl.dataframe.id`               | Unique identifier for the DataFrame execution    |
| `flow.etl.dataframe.name`             | Configured DataFrame name                        |
| `flow.etl.rows.total`                 | Total number of rows processed                   |
| `flow.etl.rows.throughput.per_second` | Processing throughput                            |
| `flow.etl.memory.min`                 | Minimum memory consumption during execution (MB) |
| `flow.etl.memory.max`                 | Maximum memory consumption during execution (MB) |

When `trace_loading` is enabled, Flow creates a child span for each loader:

- Span name: short loader class name, for example `StreamLoader`. The full name is in `flow.etl.loader.class`.
- Parent: DataFrame span
- Status: OK on success, ERROR on failure

When `trace_transformations` is enabled, Flow creates a child span for each transformer:

- Span name: short transformer class name, for example `ScalarFunctionTransformer`. The full name is in
  `flow.etl.transformer.class`. The batch sizes are in `flow.etl.transformation.input_rows` / `output_rows`.
- Parent: DataFrame span
- Status: OK on success, ERROR on failure

The same option creates one span for each processor and join (sort, group by, join, distinct):

- Span name: short class name, for example `MergeSortProcessor`. The full name is in `flow.etl.operator.class`.
- Duration: wall time from the request for the first batch to the yield of the last batch. It includes the time that
  the steps after it spend on those batches.
- Parent: DataFrame span
- Status: ERROR on failure, OK in every other case (an early stop included)

### Metrics

When `collect_metrics` is enabled:

| Metric                     | Type       | Unit    | Description                        |
|----------------------------|------------|---------|------------------------------------|
| `flow.etl.rows.processed`  | Counter    | `{row}` | Cumulative count of processed rows |
| `flow.etl.rows.throughput` | Throughput | `{row}` | Rows processed per second          |

### Logs

Flow writes structured logs at DEBUG level for these pipeline events:

- Pipeline start: with configuration details (cache type, serializer, optimizer rules, spill storages)
- Pipeline completion: with summary statistics (total rows, memory usage)
- Errors: at ERROR level, with the exception details

## Processing and Batching

Processors handle the telemetry data. You can configure them:

### Batching Processors (Recommended)

These processors collect telemetry in memory and export it in batches. This is faster:

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

These processors export each item immediately. Use them to see data in real time while you debug:

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

| Function                         | Description                                                                 |
|----------------------------------|-----------------------------------------------------------------------------|
| `always_on_sampler()`            | Record and sample every span                                                |
| `always_off_sampler()`           | Drop every span                                                             |
| `trace_id_ratio_based_sampler()` | Sample a deterministic fraction of traces                                   |
| `parent_based_sampler()`         | Use the decision of the parent. Use a root sampler for spans with no parent |
| `attribute_filtering_sampler()`  | Drop spans matching an attribute filter, defer the rest to a delegate       |

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

- `trace_loading: true`: to debug write operations or to find slow loaders, enable this option.
- `trace_transformations: true`: to debug transformation logic, enable this option for a short time. For
  high-throughput pipelines in production, disable it.
- `collect_metrics: true`: you can enable this option in production. It gives throughput data at a low cost.

### Performance Considerations

1. Use batching processors. Batching causes much less I/O than immediate export.
2. In production, disable transformation tracing. Pipelines with many transformations create many spans.
3. Set a correct batch size. Larger batches are more efficient, but they use more memory.
4. Use the curl transport. The async curl transport is faster than PSR-18 HTTP clients.
5. Call `registerShutdownFunction()`. When the script stops, it makes sure that Flow flushes all pending telemetry.

### Sampling Strategies

For high-volume pipelines, use sampling to send less telemetry:

```php
use function Flow\Telemetry\DSL\{parent_based_sampler, trace_id_ratio_based_sampler};

// Honor the parent's decision; sample 10% of traces that originate here
tracer_provider($processor, $clock, $contextStorage, parent_based_sampler(trace_id_ratio_based_sampler(0.1)));
```

The default sampler changed from `AlwaysOnSampler` to `ParentBasedSampler(AlwaysOnSampler)`. Thus Flow now drops the
spans of an unsampled parent. Refer to the [breaking change note](../libs/telemetry.md#sampling) in the documentation of the telemetry library.

### Resource Attributes

Add resource attributes that identify your service:

```php
resource([
    'service.name' => 'order-processor',
    'service.version' => '2.1.0',
    'service.namespace' => 'ecommerce',
    'deployment.environment' => 'production',
]);
```

## Disabling Telemetry

Telemetry is disabled by default. To disable it in an environment, do not give a telemetry configuration:

```php
// No telemetry - uses void providers internally
$config = config_builder()->build();
```

Or give void processors explicitly to the normal providers:

```php
use function Flow\Telemetry\DSL\{void_span_processor, void_metric_processor, void_log_processor};

$telemetry = telemetry(
    resource(['service.name' => 'my-service']),
    tracer_provider(void_span_processor(), $clock, $contextStorage),
    meter_provider(void_metric_processor(), $clock),
    logger_provider(void_log_processor(), $clock, $contextStorage),
);
```
