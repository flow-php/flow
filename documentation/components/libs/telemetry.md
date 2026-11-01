---
package: flow-php/telemetry
---

# Telemetry

Flow Telemetry is a small, OpenTelemetry-compatible observability library for PHP applications. It has one API for
distributed tracing, metrics collection, and structured logging, with minimal dependencies.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/telemetry.md).

## Telemetry

The `Telemetry` class is the main entry point for all observability operations. It manages the providers for each
signal type and gives access to Tracers, Meters, and Loggers.

> Note: The core telemetry library has contracts and basic exporters such as Console, Memory, and Void.
> To send telemetry to external backends, use
> the [OTLP Bridge](/documentation/components/bridges/telemetry-otlp-bridge.md)
> or implement custom exporters.
>
> The library gives one consistent API for telemetry, with minimal
> dependencies,
> clean contracts, and no magic.
>
> Our contracts are [here](/documentation/components/libs/telemetry/contracts.md).

### Basic Setup with Memory Providers (Testing)

```php
<?php

use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    tracer_provider,
    meter_provider,
    logger_provider,
    memory_span_processor,
    memory_metric_processor,
    memory_log_processor,
};

$resource = resource([
    'service.name' => 'test-service',
    'service.version' => '1.0.0',
]);

$spanProcessor = memory_span_processor();
$metricProcessor = memory_metric_processor();
$logProcessor = memory_log_processor();

$telemetry = telemetry(
    $resource,
    tracer_provider($spanProcessor),
    meter_provider($metricProcessor),
    logger_provider($logProcessor),
);

// Get instrumented components
$tracer = $telemetry->tracer('my-component');
$meter = $telemetry->meter('my-component');
$logger = $telemetry->logger('my-component');

// Use telemetry...
$span = $tracer->span('operation');
$tracer->complete($span);

$meter->createCounter('events', 'count')->add(1);

$logger->info('Something happened');

// Inspect collected data
$spans = $spanProcessor->endedSpans();
$metrics = $metricProcessor->metrics();
$logs = $logProcessor->entries();
```

### Scope vs Signal Attributes

`tracer()`, `meter()`, and `logger()` accept two distinct attribute sets:

- Scope attributes (`$attributes`) are attached to the instrumentation scope. All signals of the instrument share
  them without a change.
- Default signal attributes (`$signalAttributes`) are merged into each signal (each span, metric data point, or log
  record) of the instrument. Attributes from the call site override the configured defaults with the same key.

```php
<?php

use Flow\Telemetry\Attributes;

$logger = $telemetry->logger(
    'my-service',
    '1.0.0',
    attributes: Attributes::create(['service.tier' => 'backend']),        // scope
    signalAttributes: Attributes::create(['deployment.environment' => 'prod']), // every record
);

// Emitted record carries deployment.environment=prod (default) and request.id=abc (per-call).
$logger->info('request handled', ['request.id' => 'abc']);

// Per-call value wins over the default of the same key.
$logger->info('overridden', ['deployment.environment' => 'staging']);
```

### Console Output Setup (Development)

```php
<?php

use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    tracer_provider,
    meter_provider,
    logger_provider,
    pass_through_span_processor,
    pass_through_metric_processor,
    pass_through_log_processor,
    console_exporter,
};

$resource = resource([
    'service.name' => 'my-app',
    'deployment.environment' => 'development',
]);

$telemetry = telemetry(
    $resource,
    tracer_provider(pass_through_span_processor(console_exporter())),
    meter_provider(pass_through_metric_processor(console_exporter())),
    logger_provider(pass_through_log_processor(console_exporter())),
);

// All telemetry will be printed to console
$tracer = $telemetry->tracer('http-handler');
$span = $tracer->span('handle-request');
$span->setAttribute('http.method', 'GET');
$span->setAttribute('http.url', '/api/users');
$tracer->complete($span);
```

### Lifecycle Management

Always call `shutdown()` at the end of your application. This makes sure that all buffered telemetry is exported:

```php
<?php

// Register shutdown handler for graceful termination
$telemetry->registerShutdownFunction();

// Or manually flush and shutdown
$telemetry->flush();    // Export any buffered data
$telemetry->shutdown(); // Flush and close transports
```

`registerShutdownFunction()` holds only a weak reference. It does not keep the `Telemetry` instance alive. An instance
that the garbage collector removed before the end of the process is skipped. Keep a reference to the instance (for
example in your container) for as long as it must shut down at exit.

> For a production OTLP export setup, read
> the [OTLP Bridge documentation](/documentation/components/bridges/telemetry-otlp-bridge.md).

## High Level Overview

Flow Telemetry is a contract library. It defines interfaces and has basic implementations for observability. The
library works alone with the built-in Console, Memory, and Void exporters. With the
[OTLP Bridge](/documentation/components/bridges/telemetry-otlp-bridge.md), it also sends data to Application
Performance Monitoring (APM) backends.

### Architecture with OpenTelemetry Collector

The recommended production setup uses the OpenTelemetry Collector as a central hub. The Collector receives,
processes, and routes the telemetry data:

```mermaid
flowchart TB
    subgraph Applications
        A1[Order Service]
        A2[User Service]
        A3[Payment Service]
    end

    subgraph OpenTelemetry
        A1 --> C[Collector]
        A2 --> C
        A3 --> C
    end

    subgraph APM Backends
        C --> D[Grafana / Tempo]
        C --> E[Jaeger]
        C --> F[Sentry]
        C --> G[Datadog]
        C --> H[New Relic]
        C --> I[Honeycomb]
    end
```

The OpenTelemetry Collector is a vendor-agnostic proxy. It can:

- Receive telemetry from multiple sources
- Process and transform data (batching, filtering, sampling)
- Export to multiple backends at the same time

> The [official documentation](https://opentelemetry.io/docs/collector/) has more information about the
> OpenTelemetry Collector.

### Direct APM Integration

For simpler setups or specific requirements, send telemetry directly to APM backends that support OTLP:

```mermaid
flowchart TB
    subgraph Applications
        A1[Order Service]
        A2[User Service]
        A3[Payment Service]
    end

    subgraph APM Backends
        A1 --> D[Grafana Cloud]
        A2 --> E[Honeycomb]
        A3 --> F[Jaeger]
    end
```

> Important: The core `flow-php/telemetry` library has the API and basic exporters (Console, Memory, Void).
> To send telemetry to external backends, you need the `flow-php/telemetry-otlp-bridge` package. It has the OTLP
> serialization and transport.

## Signals

Telemetry collects three types of signals. Each type shows a different part of the behavior of your application.

### Traces

Traces track the flow of requests through your application and across services. A trace has one or more spans. Each
span represents a unit of work with a start time, a duration, and optional attributes.

Use cases:

- Find the bottlenecks of slow requests
- See the request flow across microservices
- Connect errors to specific operations

```mermaid
---
title: Tracing Flow
---
flowchart LR
    A[Application Code] --> B[Tracer]
    B --> C[Span]
    C --> D[SpanProcessor]
    D --> E[SpanExporter]
    E --> F[Transport]
```

Basic example:

```php
<?php

use Flow\Telemetry\Tracer\SpanKind;

$tracer = $telemetry->tracer('my-service', '1.0.0');

$span = $tracer->span('process-order', SpanKind::INTERNAL);
$span->setAttribute('order.id', '12345');
$span->setAttribute('order.total', 99.99);

// ... do work ...

$tracer->complete($span);
```

Nesting spans:

A new span does not become the current span. The OpenTelemetry specification says that span creation must not change
the active context. A span created inside another span becomes its child only after the activation of the outer span:

```php
<?php

$order = $tracer->span('process-order');
$scope = $tracer->activate($order);

try {
    // child of 'process-order'
    $tracer->complete($tracer->span('charge-card'));
} finally {
    $scope->detach();
    $tracer->complete($order);
}
```

Without `activate()`, `charge-card` is a sibling of `process-order`, not a child.

Detach scopes in the reverse order of activation, and before you complete the span. `Scope::detach()` returns one of
these values:

- `Scope::DETACHED` on success.
- `Scope::INACTIVE` for a scope that is already detached.
- `Scope::MISMATCH` for a scope that is not the innermost active scope.

Do not activate a span with the lifetime of an object instead of a block. Examples are a file stream, a database
cursor and a long-running transaction handle. Many of those are open at the same time. If you activate them, each one
becomes the parent of the next one, and you get a staircase instead of siblings. Start such spans and do not activate
them.

For automatic exception handling and span completion, use the `trace()` method:

```php
<?php

$result = $tracer->trace('fetch-user', function () use ($userId) {
    return $userRepository->find($userId);
});
```

Suppressing tracing for a region:

Set the tracing-suppression flag on the active context. All spans created in that region then do not record (they
are never sampled and never exported). This is the tracing-scoped suppression of OpenTelemetry (the equivalent of
`suppressTracing`).
Use it to silence noisy background work, for example the transport polling of a queue worker. It affects only traces.
Metrics and logs still flow.

```php
<?php

$scope = $contextStorage->attach($contextStorage->current()->withSuppressedTracing());

try {
    // Any $tracer->span(...) created here is non-recording and never exported.
    $poll();
} finally {
    $scope->detach();
}

// $contextStorage->current()->isTracingSuppressed() is false again here.
```

Use `withoutSuppressedTracing()` to remove the suppression for a nested region, for example to trace real work
inside a suppressed loop.

### URL sanitization

HTTP instrumentations (PSR-18 client, Symfony HttpClient, Symfony HttpKernel) write `url.full`, `url.query` and
exception messages through a `UrlSanitizer`. Userinfo and the values of sensitive query parameters become `REDACTED`.
The keys stay.

```php
<?php

use Flow\Telemetry\Sanitizer\UrlSanitizer;

use function Flow\Telemetry\DSL\url_sanitizer;

url_sanitizer()->url('https://u:p@graph.facebook.com/me?access_token=T&fields=id');
// https://REDACTED:REDACTED@graph.facebook.com/me?access_token=REDACTED&fields=id

url_sanitizer([...UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS, 'fb_exchange_token'])->url('https://h/me?fb_exchange_token=T');
// https://h/me?fb_exchange_token=REDACTED
```

`UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS` holds:

| Source                                                                   | Keys                                                                                                                    |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| OpenTelemetry semantic conventions                                       | `AWSAccessKeyId`, `Signature`, `sig`, `X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, `X-Goog-Signature` |
| OAuth 2.0 (RFC 6749, 6750, 7009, 7521, 7636, 8628, 8693), OpenID Connect | `access_token`, `refresh_token`, `id_token`, `id_token_hint`, `logout_token`, `client_secret`, `client_assertion`       |
| OAuth 2.0 (RFC 6749, 6750, 7009, 7521, 7636, 8628, 8693), OpenID Connect | `assertion`, `code`, `code_verifier`, `device_code`, `subject_token`, `actor_token`, `token`                            |
| API keys, credentials, sessions (Sentry and Datadog scrubbing defaults)  | `api_key`, `apikey`, `api-key`, `key`, `access_key`, `secret_key`, `private_key`, `secret`, `signature`                 |
| API keys, credentials, sessions (Sentry and Datadog scrubbing defaults)  | `password`, `passwd`, `pwd`, `auth`, `jwt`, `session`, `sessionid`, `PHPSESSID`                                         |

The list is longer than the OpenTelemetry default, which covers only signed cloud-storage URLs. Keys are
case-sensitive. A list that you give replaces the default. It does not extend it. `[]` keeps only the userinfo
redaction.

`exception.stacktrace` (spans, log records, `Throwable` attribute values) never carries function argument values.
The value of `zend.exception_ignore_args` does not change this.

### Metrics

Metrics are numerical measurements of the state and performance of your application over time. Flow Telemetry
supports four metric instruments:

| Instrument    | Description                         | Example                          |
|---------------|-------------------------------------|----------------------------------|
| Counter       | Monotonically increasing value      | Total requests, orders processed |
| UpDownCounter | Value that can increase or decrease | Active connections, queue size   |
| Gauge         | Point-in-time measurement           | CPU usage, memory consumption    |
| Histogram     | Distribution of values              | Request latency, response sizes  |

```mermaid
---
title: Metrics Flow
---
flowchart LR
    A[Application Code] --> B[Meter]
    B --> C[Instrument]
    C --> D[Metric]
    D --> E[MetricProcessor]
    E --> F[MetricExporter]
    F --> G[Transport]
```

Basic example:

```php
<?php

$meter = $telemetry->meter('my-service', '1.0.0');

$requestCounter = $meter->createCounter('http.requests.total', 'requests');
$requestCounter->add(1, ['method' => 'POST', 'status' => 200]);

$latencyHistogram = $meter->createHistogram('http.request.duration', 'ms');
$latencyHistogram->record(125.5, ['endpoint' => '/api/orders']);

$connectionGauge = $meter->createGauge('db.connections.active', 'connections');
$connectionGauge->record(42, ['pool' => 'primary']);

$queueCounter = $meter->createUpDownCounter('queue.messages', 'messages');
$queueCounter->add(1);   // message added
$queueCounter->add(-1);  // message processed
```

### Logs

Logs record discrete events with structured data and severity levels. Unlike traditional logs, telemetry logs are
connected to traces. You can therefore see log entries in the context of specific requests.

| Severity | Value | Method    |
|----------|-------|-----------|
| TRACE    | 1     | `trace()` |
| DEBUG    | 5     | `debug()` |
| INFO     | 9     | `info()`  |
| WARN     | 13    | `warn()`  |
| ERROR    | 17    | `error()` |
| FATAL    | 21    | `fatal()` |

```mermaid
---
title: Logging Flow
---
flowchart LR
    A[Application Code] --> B[Logger]
    B --> C[LogRecord]
    C --> D[LogProcessor]
    D --> E[LogExporter]
    E --> F[Transport]
```

Basic example:

```php
<?php

$logger = $telemetry->logger('my-service', '1.0.0');

$logger->info('Order processed successfully', [
    'order_id' => '12345',
    'customer_id' => 'cust-789',
    'total' => 99.99,
]);

$logger->error('Payment failed', [
    'order_id' => '12345',
    'error_code' => 'CARD_DECLINED',
]);

$logger->debug('Cache hit', ['key' => 'user:123', 'ttl' => 3600]);
```

## Attribute and Cardinality Limits

Flow Telemetry enforces limits on signals, as the OpenTelemetry specification says. The limits stop unbounded memory
growth, and they make sure that telemetry data stays within the size limits of the collector.

### Default Limits

| Signal Type | Attribute Count | Value Length | Events | Links | Cardinality |
|-------------|-----------------|--------------|--------|-------|-------------|
| Span        | 128             | unlimited    | 128    | 128   | N/A         |
| Span Event  | 128             | unlimited    | -      | -     | N/A         |
| Span Link   | 128             | unlimited    | -      | -     | N/A         |
| LogRecord   | 128             | unlimited    | -      | -     | N/A         |
| Metric      | exempt          | exempt       | -      | -     | 2000        |

Note: The OTel specification exempts resource attributes from limits. Metrics are exempt from the attribute count
and value length limits, but they have cardinality limits (unique attribute combinations for each instrument).

### Configuring Limits

```php
<?php

use function Flow\Telemetry\DSL\{span_limits, log_record_limits, metric_limits, tracer_provider, logger_provider, meter_provider};

// Custom span limits
$spanLimits = span_limits(
    attributeCountLimit: 64,
    eventCountLimit: 32,
    linkCountLimit: 16,
    attributeValueLengthLimit: 1024,  // truncate strings longer than 1024 chars
);

$tracerProvider = tracer_provider(
    $processor,
    $clock,
    $contextStorage,
    limits: $spanLimits,
);

// Custom log limits
$logLimits = log_record_limits(
    attributeCountLimit: 64,
    attributeValueLengthLimit: 2048,
);

$loggerProvider = logger_provider(
    $processor,
    $clock,
    $contextStorage,
    limits: $logLimits,
);

// Custom metric limits (cardinality only)
$metricLimits = metric_limits(
    cardinalityLimit: 1000,  // max unique attribute combinations per instrument
);

$meterProvider = meter_provider(
    $processor,
    $clock,
    limits: $metricLimits,
);
```

### Enforcement Behavior

- Attribute count exceeded: new attributes are discarded (the first N stay)
- String value too long: values are truncated to the configured limit
- Array values: each string element is truncated separately

The counts of dropped attributes are tracked for each signal and included in the exported telemetry data.

### Metric Cardinality Overflow

When a metric instrument goes over its cardinality limit, new attribute combinations go to an overflow
aggregator. The overflow data point has one attribute:
`otel.metric.overflow: true`. All measurements with new attribute combinations past the limit are
aggregated into this overflow data point.

This follows the OpenTelemetry specification for metrics overflow. Measurements are never dropped. They are aggregated
into the overflow bucket.

## Processors

Processors handle the lifecycle of telemetry signals. They decide the time and the way that data goes to exporters. They are
between the instrumentation (Tracer, Meter, Logger) and the exporters.

```mermaid
---
title: Processor Flow
---
flowchart LR
    subgraph Signal Creation
        A[Tracer/Meter/Logger]
    end

    subgraph Processing
        A --> B[Processor]
        B -->|buffering| B
    end

    subgraph Export
        B -->|batch ready| C[Exporter]
    end
```

### Processor Types

| Type        | Behavior                                   | Use Case                                     |
|-------------|--------------------------------------------|----------------------------------------------|
| PassThrough | Exports immediately on every signal        | Development, debugging, immediate visibility |
| Batching    | Buffers signals, exports at the batch size | Production, performance optimization         |
| Memory      | Stores signals in memory for inspection    | Testing, assertions                          |
| Void        | Discards all signals (no-op)               | Disabled telemetry, library defaults         |

Batching processor example:

The batching processor collects signals up to a configured batch size. Then it exports them all at the same time.
This reduces the network overhead and improves the performance in production environments.

```php
<?php

use function Flow\Telemetry\DSL\batching_span_processor;
use function Flow\Telemetry\DSL\console_exporter;

$processor = batching_span_processor(
    console_exporter(),
    batchSize: 512
);

// Spans are buffered until 512 are collected, then exported together
// Call flush() to export immediately regardless of batch size
$processor->flush();
```

Each signal type has its own processor implementations:

- `PassThroughSpanProcessor`, `BatchingSpanProcessor`, `MemorySpanProcessor`, `VoidSpanProcessor`
- `PassThroughMetricProcessor`, `BatchingMetricProcessor`, `MemoryMetricProcessor`, `VoidMetricProcessor`
- `PassThroughLogProcessor`, `BatchingLogProcessor`, `MemoryLogProcessor`, `VoidLogProcessor`

### Filtering by attributes

Attribute filtering drops (or keeps) signals by their attributes. This reduces noise, for example health-check spans
or bot traffic, before it reaches an exporter. For spans and metrics, it is a processor that wraps an inner processor.
For logs, it is a middleware in a [log pipeline](#log-processing-pipeline) (read below). Both send only the signals
that pass an `AttributeFilter`.

An `AttributeFilter` evaluates one root matcher. The leaf matcher, `attribute_rule()`, targets an attribute
path and applies a `MatchMode`:

| Family   | Modes                                                                                      |
|----------|--------------------------------------------------------------------------------------------|
| Equality | `EQUAL`, `NOT_EQUAL` (strict comparison against the expected value)                        |
| Ordering | `GREATER_THAN`, `GREATER_THAN_EQUAL`, `LESS_THAN`, `LESS_THAN_EQUAL`                       |
| Pattern  | `REGEXP`, `STARTS_WITH`, `ENDS_WITH`, `CONTAINS` (operate on the string form of the value) |

A path is one segment (a top-level attribute key), or many segments that go into nested array values, for example
`['user', 'id']`. The pattern modes accept a `caseSensitive` flag for each rule (default `true`).

Rules combine with the `all()`, `any()` and `not()` matchers. Each of them is also a matcher, so they nest freely:

```php
<?php

use Flow\Telemetry\Filter\MatchMode;

use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_span_processor;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\batching_span_processor;
use function Flow\Telemetry\DSL\console_exporter;

// Drop health-check spans OR any span faster than 5ms before they are exported
$processor = attribute_filtering_span_processor(
    batching_span_processor(console_exporter()),
    attribute_filter(
        any(
            attribute_rule('http.route', MatchMode::EQUAL, '/health'),
            attribute_rule(['timing', 'duration_ms'], MatchMode::LESS_THAN, 5),
        ),
    )
);
```

You can give a single rule directly, without `all()`/`any()` around it:

```php
$filter = attribute_filter(attribute_rule('http.route', MatchMode::EQUAL, '/health'));
```

By default, a match excludes the signal (drops it). Set `exclude: false` to keep only matching signals. With
`sources`, the filter looks at resource attributes, scope attributes, or both, instead of the attributes of the signal
itself. `sources` is a list. The matcher runs against each source, and the results are OR-combined. One match in one listed source
is sufficient for a signal to match:

```php
<?php

use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;

use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\not;

// Keep ONLY metrics from the payments scope that are not internal
$filter = attribute_filter(
    all(
        attribute_rule('name', MatchMode::EQUAL, 'app.payments'),
        not(attribute_rule('internal', MatchMode::EQUAL, true)),
    ),
    exclude: false,
    sources: [AttributeSource::SCOPE],
);
```

The matcher tree is compiled to a small PHP matcher and cached on disk. The default location is the system temp
directory. You can give a different directory as `cacheDir`. The evaluation for each signal therefore stays cheap.
When the cache directory is not writable, or a matcher cannot be compiled, the processor uses interpreted matching.
Custom matchers implement the `Matcher` interface, and optionally
`CompilableMatcher` to take part in code generation.

> Security: the compiled matcher is `require`d. The cache directory must therefore be trusted, and untrusted users
> must never be able to write to it. Rule values are escaped (with `var_export`), and signal attributes are
> never written into the generated code. Neither of them can therefore inject PHP. The risk is a shared, writable cache
> directory, where another user can put code at the deterministic file path first. The default
> (`sys_get_temp_dir()`) is world-accessible. In multi-tenant environments, give an application-private `cacheDir`.
> The directory is created with `0700` (owner-only). To widen it, give `cacheDirPermissions`, for example `0750` for a
> different reader user in the same group. The value must be between `0` and `0777`. It applies only at the creation of the
> directory.

For spans and metrics, filtering is a processor that wraps an inner processor:
`AttributeFilteringSpanProcessor` / `AttributeFilteringMetricProcessor`. You create them with
`attribute_filtering_span_processor()` and `attribute_filtering_metric_processor()`.

### Log processing pipeline

A pipeline of ordered middleware processes the logs, and it ends in one sink. This is the same as the OpenTelemetry
`LogRecordProcessor` model. Each step can change the record, which the specification explicitly allows, or drop it.
The next step sees the changes. A `LogMiddleware` returns the entry for the next step (possibly enriched), or `null` to
drop it. A `LogSink` (batching, pass-through, memory, void, composite) exports the remaining entries.

```php
<?php

use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Logger\Severity;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_log_middleware;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\batching_log_processor;
use function Flow\Telemetry\DSL\enriching_log_middleware;
use function Flow\Telemetry\DSL\pipeline_log_processor;
use function Flow\Telemetry\DSL\severity_filtering_log_middleware;

// enrich -> drop below WARN -> drop health checks -> batch to the exporter
$processor = pipeline_log_processor(
    [
        enriching_log_middleware(['deployment.environment' => 'prod']),
        severity_filtering_log_middleware(Severity::WARN),
        attribute_filtering_log_middleware(attribute_filter(
            attribute_rule('http.route', MatchMode::EQUAL, '/health'),
        )),
    ],
    batching_log_processor($exporter),
);
```

The middleware are `EnrichingLogMiddleware`, `AttributeFilteringLogMiddleware` and `SeverityFilteringLogMiddleware`
(`enriching_log_middleware()`, `attribute_filtering_log_middleware()`, `severity_filtering_log_middleware()`).
`pipeline_log_processor()` combines them into a `PipelineLogProcessor`. The attributes of `EnrichingLogMiddleware` are
defaults. Attributes set at the call site win over them. To add your own step, implement `LogMiddleware`.

## Sampling

At span start, a `Sampler` decides about the record and the export of a span. This is the OpenTelemetry way to drop spans. A
dropped span never records, so it never reaches a processor or exporter.

> Breaking change. The default sampler of `TracerProvider` and the `tracer_provider()` DSL helper is now
> `ParentBasedSampler(AlwaysOnSampler)` instead of `AlwaysOnSampler`. This matches the OpenTelemetry SDK
> default `parentbased_always_on`. Spans with an unsampled parent are now dropped. For the previous behaviour,
> give `always_on_sampler()` explicitly.

| Sampler                     | DSL                                               | Behavior                                                                                                   |
|-----------------------------|---------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `AlwaysOnSampler`           | `always_on_sampler()`                             | Record + sample every span                                                                                 |
| `AlwaysOffSampler`          | `always_off_sampler()`                            | Drop every span                                                                                            |
| `TraceIdRatioBasedSampler`  | `trace_id_ratio_based_sampler($ratio)`            | Sample a deterministic fraction of traces                                                                  |
| `ParentBasedSampler`        | `parent_based_sampler($root)`                     | Honor the decision of the parent, and use the root sampler for spans without a parent (the default)        |
| `AttributeFilteringSampler` | `attribute_filtering_sampler($filter, $delegate)` | Drop spans with start-time attributes that match an `AttributeFilter`. A delegate sampler decides the rest |

`AttributeFilteringSampler` uses the same matcher stack as the [filtering processors](#filtering-by-attributes): the
`AttributeFilter` (matcher tree, compiled drop-closure, `sources`, `exclude` polarity). A match drops the span (the
default), or it keeps only matching spans (`exclude: false`). The `$delegate` sampler decides all other spans, so the
sampler combines with ratio/parent-based sampling. It sees only the attributes at span start. It does not see
end-state attributes (status codes, durations), so filtering on end attributes stays
a [span processor](#filtering-by-attributes)
task.

```php
<?php

use Flow\Telemetry\Filter\MatchMode;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_sampler;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\trace_id_ratio_based_sampler;

// Drop health-check spans; sample everything else at 10%
$sampler = attribute_filtering_sampler(
    attribute_filter(attribute_rule('http.route', MatchMode::EQUAL, '/health')),
    trace_id_ratio_based_sampler(0.1),
);
```

## Exporters

Exporters send telemetry data to external systems. In the telemetry architecture, exporters receive batches of
signals from processors and send them with a transport mechanism.

An exporter usually needs two components:

- Serializer: converts signals to a wire format (JSON, Protobuf)
- Transport: handles the network communication (HTTP, gRPC)

> Note: The core telemetry library has basic exporters. For OTLP-compatible serializers and transports, read
> the [OTLP Bridge documentation](/documentation/components/bridges/telemetry-otlp-bridge.md).

### Built-in Exporters

The telemetry library has three types of exporters for different use cases:

#### Memory Exporters

Store signals in memory for direct access. Use them for tests and debugging.

```php
<?php

use function Flow\Telemetry\DSL\memory_exporter;
use function Flow\Telemetry\DSL\pass_through_span_processor;

$exporter = memory_exporter();
$processor = pass_through_span_processor($exporter);

// ... create and complete spans ...

$spans = $exporter->spans();
assert(count($spans) === 3);
assert($spans[0]->name === 'my-operation');
```

#### Console Exporters

Write human-readable telemetry to the console, with optional ANSI colors. Use them for development and debugging.

```php
<?php

use function Flow\Telemetry\DSL\console_exporter;
use function Flow\Telemetry\DSL\pass_through_span_processor;

$exporter = console_exporter(colors: true);
$processor = pass_through_span_processor($exporter);
```

The console output shows spans in a formatted ASCII table with trace IDs, durations, attributes, and events.

#### Void Exporters

No-op implementations that discard all data. Use them as defaults in libraries, or for disabled telemetry.

```php
<?php

use function Flow\Telemetry\DSL\void_span_processor;

// No performance impact - signals are discarded immediately
$processor = void_span_processor();
```

> Performance Tip: In libraries, use Void processors as defaults. If the application does not configure telemetry,
> the overhead is then zero. The application can still enable the instrumentation.

## Propagators

Propagators make distributed tracing possible. They extract and inject trace context across service boundaries, for
example HTTP headers and message queues.

Flow Telemetry has two W3C-standard propagators:

- W3CTraceContext: propagates trace IDs and span IDs through the `traceparent` and `tracestate` headers
- W3CBaggage: propagates application-specific key-value pairs through the `baggage` header

```php
<?php

use function Flow\Telemetry\DSL\composite_propagator;
use function Flow\Telemetry\DSL\array_carrier;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Propagation\W3CBaggage;

$propagator = composite_propagator([
    new W3CTraceContext(),
    new W3CBaggage(),
]);

// Extract context from incoming request
$carrier = array_carrier($_SERVER);
$context = $propagator->extract($carrier);

// Inject context into outgoing request
$headers = [];
$carrier = array_carrier($headers);
$propagator->inject($context, $carrier);
```

### Resource

A Resource represents the entity that produces telemetry: your application, service, or component. Its attributes
identify the origin of the telemetry data.

Common resource attributes:

- `service.name`: the logical name of the service
- `service.version`: the version of the service
- `host.name`: the hostname of the machine
- `process.pid`: the process ID

```php
<?php

use function Flow\Telemetry\DSL\resource;

$resource = resource([
    'service.name' => 'order-service',
    'service.version' => '2.1.0',
    'deployment.environment' => 'production',
]);
```

The Resource goes to the Telemetry instance, which attaches it to all spans, metrics, and logs. APM backends can then
filter and group telemetry by service, environment, or any other attribute.

### Resource Detectors

Resource detectors find information about the entity that produces telemetry. You do not have to set the resource
attributes yourself. The detectors look at the environment and fill in attributes such as OS type, hostname, process
ID, and service information.

#### Default Detection

The simplest way to detect resources is the `resource_detector()` function. It gives a default chain of
detectors:

```php
<?php

use function Flow\Telemetry\DSL\resource_detector;

$resource = resource_detector()->detect();
```

This returns a `Resource` with attributes from OS, host, process, Composer, and environment variables.

#### Available Detectors

| Detector            | DSL Function                         | Detected Attributes                                                                                                             |
|---------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| OsDetector          | `os_detector()`                      | `os.type`, `os.name`, `os.version`, `os.description`                                                                            |
| HostDetector        | `host_detector()`                    | `host.name`, `host.arch`, `host.id`                                                                                             |
| ProcessDetector     | `process_detector()`                 | `process.pid`, `process.executable.path`, `process.runtime.name`, `process.runtime.version`, `process.command`, `process.owner` |
| ComposerDetector    | `composer_detector()`                | `service.name`, `service.version`                                                                                               |
| EnvironmentDetector | `environment_detector()`             | Reads `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`                                                                           |
| StaticDetector      | `static_detector($attributes)`       | User-specified attributes                                                                                                       |
| CachingDetector     | `caching_detector($detector, $path)` | Caches the result of the wrapped detector to file                                                                               |
| ChainDetector       | `chain_detector(...$detectors)`      | Combines multiple detectors                                                                                                     |

#### Custom Detection Chain

Use `chain_detector()` to combine specific detectors. For conflicting attribute keys, later detectors win over
earlier detectors:

```php
<?php

use function Flow\Telemetry\DSL\{chain_detector, os_detector, host_detector, static_detector};

$resource = chain_detector(
    os_detector(),
    host_detector(),
    static_detector(['service.name' => 'my-api', 'service.version' => '2.0.0']),
)->detect();
```

#### Manual Attributes

For full control over resource attributes:

```php
<?php

use function Flow\Telemetry\DSL\static_detector;

$resource = static_detector([
    'service.name' => 'order-service',
    'service.version' => '1.0.0',
    'deployment.environment.name' => 'production',
])->detect();
```

#### Caching Detection Results

Resource detection can read files, for example `/etc/machine-id` for the host ID. Use `caching_detector()` to cache
the results and not detect again:

```php
<?php

use function Flow\Telemetry\DSL\{caching_detector, resource_detector};

$resource = caching_detector(resource_detector())->detect();
```

By default, the cache is in `sys_get_temp_dir()/flow_telemetry_resource.cache`. You can give a custom path as the
second argument.

#### Environment Variables

The `EnvironmentDetector` reads standard OpenTelemetry environment variables:

- `OTEL_SERVICE_NAME`: sets the `service.name` attribute
- `OTEL_RESOURCE_ATTRIBUTES`: sets additional attributes in `key=value,key2=value2` format

The OpenTelemetry Resource SDK specification says that `,` and `=` in keys and values must be percent-encoded.
Other characters can be percent-encoded. Keys and values are both percent-decoded. For example,
`note=a%2Cb` gives the attribute `note` with the value `a,b`.

```bash
export OTEL_SERVICE_NAME=my-service
export OTEL_RESOURCE_ATTRIBUTES=service.version=1.0.0,deployment.environment.name=production
```

These environment variables have the highest precedence in the default detector chain. You can therefore change the
configuration at runtime without code changes.

### InstrumentationScope

An InstrumentationScope identifies the library or component that generates telemetry. APM backends can then connect
telemetry to specific instrumentation, and filter by library version.

```php
<?php

// InstrumentationScope is created automatically when you get a tracer/meter/logger
$tracer = $telemetry->tracer(
    name: 'http-client',
    version: '1.2.0',
    schemaUrl: 'https://opentelemetry.io/schemas/1.21.0'
);
```

The scope has:

- name: name of the instrumentation library, for example `http-client`, `database` or `queue`
- version: version of the instrumentation
- schemaUrl: optional URL of the semantic convention schema
