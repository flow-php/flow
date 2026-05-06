# Symfony Telemetry Bundle

Flow Symfony Telemetry Bundle provides automatic telemetry integration for Symfony applications, including HTTP
request/response tracing, console command instrumentation, and configurable exporters through OpenTelemetry-compatible
backends.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-telemetry-bundle)
- [➡️ Installation](/documentation/installation/packages/symfony-telemetry-bundle.md)
- [GitHub](https://github.com/flow-php/symfony-telemetry-bundle)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/symfony-telemetry-bundle.md).

## Overview

This bundle is built on top of [flow-php/telemetry](/documentation/components/libs/telemetry.md) — see that page for the underlying `Telemetry`, tracer/meter/logger API, and processor/exporter primitives. For exporting to OTLP-compatible backends, it also uses the [Telemetry OTLP Bridge](/documentation/components/bridges/telemetry-otlp-bridge.md).

This bundle integrates Flow PHP's Telemetry library with Symfony applications. It provides:

- **Automatic resource detection** - Detects service name, version, environment, OS, host, and process information
- **8 auto-instrumentation options** - HTTP kernel, console, messenger, Twig, HTTP client, PSR-18 client, Doctrine DBAL,
  and cache
- **Context propagation** - W3C TraceContext and Baggage support for distributed tracing
- **Configurable exporters** - Console, memory, void, or OTLP with multiple transport options
- **Full Symfony configuration** - Configure everything through Symfony's config system

## Configuration Reference

### Resource Configuration

The `resource` node configures OpenTelemetry Resource attributes that identify your service.

```yaml
flow_telemetry:
  resource:
    detectors:
      enabled: true  # Enable resource detectors (default: true)
      static:
        cache:
          enabled: true  # Cache static attributes (default: true)
          path: null     # Cache file path (default: sys_get_temp_dir()/flow_telemetry_resource.cache)
        os:
          enabled: true  # Detect os.type, os.name, os.version, os.description
        host:
          enabled: true  # Detect host.name, host.arch, host.id
        service:
          enabled: true  # Detect service.name, service.version from composer.json
        deployment:
          enabled: true  # Detect deployment.environment.name from kernel environment
        environment:
          enabled: true  # Read OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES
      dynamic:
        process:
          enabled: true  # Detect process.pid, process.runtime.*, process.executable.*
    custom:
      service.name: 'my-app'
      service.version: '1.0.0'
      deployment.environment.name: 'production'
```

**Detector types:**

| Type          | Class                       | Category | Attributes Detected                                               |
|---------------|-----------------------------|----------|-------------------------------------------------------------------|
| `os`          | `OsDetector`                | static   | `os.type`, `os.name`, `os.version`, `os.description`              |
| `host`        | `HostDetector`              | static   | `host.name`, `host.arch`, `host.id`                               |
| `service`     | `ComposerDetector`          | static   | `service.name`, `service.version` (from composer.json)            |
| `deployment`  | `SymfonyDeploymentDetector` | static   | `deployment.environment.name` (from kernel environment)           |
| `environment` | `EnvironmentDetector`       | static   | Reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` env vars |
| `process`     | `ProcessDetector`           | dynamic  | `process.pid`, `process.runtime.*`, `process.executable.*`        |

Static detectors are cached by default. Dynamic detectors run on every request/command.

The cache file lives outside Symfony's cache lifecycle on purpose: building the Symfony cache
(via `cache:warmup`) at image build time would otherwise freeze runtime-dependent attributes
such as `host.name` or `process.pid` from the build container. Defaulting to
`sys_get_temp_dir()` keeps the cache per-runtime and avoids that pitfall. To invalidate it,
delete the cache file or restart the process; `cache:clear` does not touch it.

Custom attributes override auto-detected values.

### Clock Configuration

- **type**: `string|null`
- **default**: `null`

Custom PSR-20 clock service ID. If not provided, uses the built-in SystemClock.

```yaml
flow_telemetry:
  clock_service_id: 'app.clock'
```

### Context Storage

- **type**: `enum`
- **default**: `memory`

Context storage for maintaining trace context across async operations.

```yaml
flow_telemetry:
  context_storage:
    type: memory  # memory|service
    service_id: null  # Custom service ID (only for type: service)
```

### Propagator

- **type**: `enum`
- **default**: `w3c`

Context propagator for distributed tracing. Determines how trace context is injected/extracted from HTTP headers.

```yaml
flow_telemetry:
  propagator:
    type: w3c  # w3c|tracecontext|baggage|service
    service_id: null  # Custom service ID (only for type: service)
```

| Type           | Description                              |
|----------------|------------------------------------------|
| `w3c`          | W3C TraceContext + Baggage (recommended) |
| `tracecontext` | W3C TraceContext only                    |
| `baggage`      | W3C Baggage only                         |
| `service`      | Custom propagator service                |

### Exporters (named definitions)

The bundle exposes a **top-level named map** of exporters. The implementation is selected by the **sub-block name**
under each entry — there is no separate `type:` key. Each exporter must declare exactly one of the supported
sub-blocks: `otlp`, `service`, `console`, `memory`, `void`. Per-signal processors reference exporters by name.

```yaml
flow_telemetry:
  exporters:
    otlp:                              # exporter name
      otlp:                            # sub-block selects implementation
        transport:
          type: curl
          endpoint: 'http://otel-collector:4318'
          serializer: { type: protobuf }
```

| Sub-block | Options                                | Description                              |
|-----------|----------------------------------------|------------------------------------------|
| `otlp`    | `transport: { ... }`                   | Sends batches over OTLP curl/grpc/service |
| `service` | `id: <service_id>`                     | Aliases an existing user-provided service |
| `console` | none (use `~` / `null` / `{}`)          | Pretty-prints to console                  |
| `memory`  | none                                   | Stores batches in memory (testing)        |
| `void`    | none                                   | Discards everything (no-op)               |

Service IDs registered by the bundle (predictable for `decorates:`):

- `flow.telemetry.exporter.<name>` — e.g. `flow.telemetry.exporter.otlp`
- `flow.telemetry.exporter.<name>.transport` — only when the exporter uses the `otlp` sub-block

### TracerProvider

Configures the tracer provider for distributed tracing.

```yaml
flow_telemetry:
  tracer_provider:
    sampler:
      type: always_on   # always_on|always_off|trace_id_ratio|parent_based|service
      ratio: 1.0        # Sampling ratio (0.0-1.0, only for trace_id_ratio)
      service_id: null  # Custom sampler service (only for type: service)
    processor:
      type: batching    # composite|memory|batching|passthrough|void|service
      batch_size: 512
      exporter: otlp    # name of a top-level exporter
```

**Sampler types:**

| Type             | Description                             |
|------------------|-----------------------------------------|
| `always_on`      | Sample all traces (default)             |
| `always_off`     | Sample no traces                        |
| `trace_id_ratio` | Sample based on trace ID ratio          |
| `parent_based`   | Respect parent span's sampling decision |
| `service`        | Custom sampler service                  |

### MeterProvider

```yaml
flow_telemetry:
  meter_provider:
    temporality: cumulative  # cumulative|delta
    processor:
      type: batching
      batch_size: 512
      exporter: otlp
```

### LoggerProvider

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: batching   # composite|memory|batching|passthrough|void|severity_filtering|service
      batch_size: 512
      exporter: otlp
```

**Severity filtering** (logs only):

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: severity_filtering
      minimum_severity: warn  # trace|debug|info|warn|error|fatal
      inner_processor:
        type: batching
        exporter: otlp
        batch_size: 200
```

### Processor Configuration

Processor types available for `tracer_provider`, `meter_provider`, and `logger_provider`.

#### void (default)

Discards all data. No additional options.

```yaml
processor:
  type: void
```

#### passthrough

Immediately exports each item via the referenced exporter.

```yaml
processor:
  type: passthrough
  exporter: console
```

#### memory

Stores in memory (for testing). Still requires a backing exporter for `flush()` to call.

```yaml
processor:
  type: memory
  exporter: memory
```

#### batching

Batches items before export.

| Option       | Type    | Default | Description                                  |
|--------------|---------|---------|----------------------------------------------|
| `batch_size` | integer | `512`   | Number of items per batch                    |
| `exporter`   | string  | -       | Name of a top-level exporter (required)      |

```yaml
processor:
  type: batching
  batch_size: 512
  exporter: otlp
```

#### composite

Combines multiple processors. Each child references its own exporter by name.

```yaml
processor:
  type: composite
  processors:
    - { type: batching, exporter: otlp, batch_size: 512 }
    - { type: memory,   exporter: memory }
```

#### service

Custom processor service.

```yaml
processor:
  type: service
  service_id: 'app.custom_processor'
```

#### severity_filtering (logger_provider only)

Filters logs by minimum severity level. The wrapped `inner_processor` is built with the same set of types as a top-level
processor (except `composite`/`severity_filtering`).

```yaml
processor:
  type: severity_filtering
  minimum_severity: warn
  inner_processor:
    type: batching
    exporter: otlp
    batch_size: 200
```

### Exporter Definitions

Exporters are declared once at the top level under `exporters:` and referenced from processor blocks by name. Each
exporter declares exactly one sub-block; the sub-block name selects the implementation.

#### void

Discards all data.

```yaml
exporters:
  drop: { void: ~ }
```

#### memory

In-memory store for testing. Service exposes `allLogs()`, `allMetrics()`, `allSpans()` accessors via
`Flow\Telemetry\Provider\Memory\MemoryExporter`.

```yaml
exporters:
  capture: { memory: ~ }
```

#### console

Pretty-prints logs, metrics, and spans to the console. Useful for development.

```yaml
exporters:
  debug: { console: ~ }
```

#### otlp

Sends batches over an embedded transport. The single OTLP exporter handles all three signals.

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        serializer: { type: protobuf }
```

#### service

Aliases an existing user-defined service implementing `Flow\Telemetry\Exporter\Exporter`. This is the escape hatch for
APM-specific exporters (Datadog, New Relic, custom) that don't go through OTLP — define your exporter as a Symfony
service in your own `services.yaml` and reference it by id.

```yaml
exporters:
  datadog:
    service:
      id: 'app.datadog_telemetry_exporter'
```

### OTLP Transport Configuration

Inside `exporters.<name>.otlp.transport`. Required for the `otlp` sub-block.

#### curl (default)

| Option             | Type    | Default | Description                 |
|--------------------|---------|---------|-----------------------------|
| `endpoint`         | string  | -       | OTLP base URL (required)    |
| `timeout`          | integer | `30`    | Request timeout in seconds  |
| `connect_timeout`  | integer | `10`    | Connection timeout          |
| `compression`      | boolean | `false` | Enable compression          |
| `follow_redirects` | boolean | `true`  | Follow HTTP redirects       |
| `max_redirects`    | integer | `3`     | Maximum redirects to follow |
| `proxy`            | string  | `null`  | Proxy URL                   |
| `ssl_verify_peer`  | boolean | `true`  | Verify SSL peer             |
| `ssl_verify_host`  | boolean | `true`  | Verify SSL host             |
| `ssl_cert_path`    | string  | `null`  | SSL certificate path        |
| `ssl_key_path`     | string  | `null`  | SSL key path                |
| `ca_info_path`     | string  | `null`  | CA info path                |
| `headers`          | object  | `{}`    | Additional HTTP headers     |
| `serializer`       | object  | `json`  | OTLP serializer config      |

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        timeout: 30
        compression: true
        headers:
          Authorization: 'Bearer token'
        serializer: { type: protobuf }
```

#### grpc

gRPC transport (`timeout` is rejected by validation).

| Option     | Type    | Default | Description                |
|------------|---------|---------|----------------------------|
| `endpoint` | string  | -       | gRPC endpoint (required)   |
| `insecure` | boolean | `true`  | Allow insecure connections |
| `headers`  | object  | `{}`    | gRPC metadata              |

```yaml
exporters:
  otlp_grpc:
    otlp:
      transport:
        type: grpc
        endpoint: 'http://otel-collector:4317'
        insecure: false
        serializer: { type: protobuf }
```

#### service

Aliases an existing transport service ID inside the OTLP exporter.

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: service
        service_id: 'app.custom_transport'
```

### OTLP Serializer Configuration

Inside `exporters.<name>.otlp.transport.serializer`. Choices: `json` (default), `protobuf`, `service`.

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        serializer:
          type: service
          service_id: 'app.custom_serializer'
```

### Multiple OTLP backends

Each signal can target its own collector by declaring multiple named exporters and referencing them per provider.

```yaml
flow_telemetry:
  exporters:
    otlp_traces:
      otlp:
        transport:
          type: grpc
          endpoint: 'http://traces:4317'
          insecure: false
          serializer: { type: protobuf }
    otlp_metrics:
      otlp:
        transport:
          type: curl
          endpoint: 'http://metrics:4318'
          serializer: { type: protobuf }
    otlp_logs:
      otlp:
        transport:
          type: curl
          endpoint: 'http://logs:4318'
          serializer: { type: json }

  tracer_provider:
    processor: { type: batching, exporter: otlp_traces,  batch_size: 1024 }
  meter_provider:
    processor: { type: batching, exporter: otlp_metrics, batch_size: 256  }
  logger_provider:
    processor: { type: batching, exporter: otlp_logs,    batch_size: 100  }
```

### Migrating from older config

The schema replaced the `type: <name>` discriminator with a sub-block whose key matches the implementation. There is no
BC shim.

**Before (legacy schema)**

```yaml
flow_telemetry:
  exporters:
    otlp:
      type: otlp
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        serializer: { type: protobuf }
    custom:
      type: service
      service_id: 'app.x'
    debug: { type: console }
```

**After**

```yaml
flow_telemetry:
  exporters:
    otlp:
      otlp:
        transport:
          type: curl
          endpoint: 'http://otel-collector:4318'
          serializer: { type: protobuf }
    custom:
      service:
        id: 'app.x'
    debug: { console: ~ }

  tracer_provider:
    processor: { type: batching, exporter: otlp }
```

### Instrumentation

Configure automatic instrumentation for various Symfony components.

**All instrumentation is disabled by default.** You must explicitly set `enabled: true` for each component you want to
instrument.

#### HTTP Kernel

Traces HTTP requests and responses.

```yaml
flow_telemetry:
  instrumentation:
    http_kernel:
      enabled: true
      context_propagation: true  # Extract context from incoming headers
      exclude_paths:
        - path: '/_profiler'
        - path: '/_wdt'
        - path: '/health'
          method: GET
        - path: '/^\/api\/internal\/.*/'  # Regex pattern
```

#### Console

Traces console commands.

```yaml
flow_telemetry:
  instrumentation:
    console:
      enabled: true
      exclude_commands:
        - 'cache:clear'
        - 'assets:install'
        - '/^debug:.*/'  # Regex: exclude all debug commands
```

#### Messenger

Traces Symfony Messenger messages with context propagation across message boundaries.

```yaml
flow_telemetry:
  instrumentation:
    messenger:
      enabled: true
      context_propagation: true  # Propagate context across message boundaries
```

#### Twig

Traces Twig template rendering.

```yaml
flow_telemetry:
  instrumentation:
    twig:
      enabled: true
      trace_templates: true   # Trace template rendering
      trace_blocks: false     # Trace block rendering
      trace_macros: false     # Trace macro execution
      exclude_templates:
        - '@WebProfiler'
        - '/^@Debug\/.*/'
```

#### HTTP Client

Traces Symfony HTTP Client requests.

```yaml
flow_telemetry:
  instrumentation:
    http_client:
      enabled: true
      exclude_clients:
        - 'internal.client'
        - '/^debug\..*/'
```

#### PSR-18 Client

Traces PSR-18 HTTP client requests.

```yaml
flow_telemetry:
  instrumentation:
    psr18_client:
      enabled: true
      exclude_clients:
        - 'app.internal_client'
```

#### Doctrine DBAL

Traces database queries.

```yaml
flow_telemetry:
  instrumentation:
    dbal:
      enabled: true
      log_sql: true           # Include SQL in span attributes
      max_sql_length: 1000    # Max SQL length (0 = no limit)
      exclude_connections:
        - 'legacy'
        - '/^test_.*/'
```

#### Cache

Traces Symfony Cache operations.

```yaml
flow_telemetry:
  instrumentation:
    cache:
      enabled: true
      exclude_pools:
        - 'cache.system'
        - '/^cache\.validator.*/'
```

### Named Instruments

Configure named tracers, meters, and loggers with custom instrumentation scope attributes.

**Options:**

| Option       | Type   | Default     | Description                         |
|--------------|--------|-------------|-------------------------------------|
| `version`    | string | `'unknown'` | Instrumentation scope version       |
| `schema_url` | string | `null`      | Schema URL for semantic conventions |
| `attributes` | object | `{}`        | Additional scope attributes         |

```yaml
flow_telemetry:
  tracers:
    my_tracer:
      version: '1.0.0'  # default: 'unknown'
      schema_url: 'https://opentelemetry.io/schemas/1.21.0'
      attributes:
        custom.attribute: 'value'

  meters:
    my_meter:
      version: '1.0.0'  # default: 'unknown'
      schema_url: null
      attributes: { }

  loggers:
    my_logger:
      version: '1.0.0'  # default: 'unknown'
```

### Main Logger

The bundle depends on [PSR-3 Telemetry Bridge](/documentation/components/bridges/psr3-telemetry-bridge.md) and registers a PSR-3 wrapper service for every named Telemetry logger at `flow.telemetry.<name>.logger.psr3`. This makes Flow Telemetry loggers usable as Symfony's `logger` service, removing the need for Monolog when telemetry is the only logging destination.

In addition, the bundle always registers a `default` logger, meter, and tracer — `flow.telemetry.default.logger`, `flow.telemetry.default.logger.psr3`, `flow.telemetry.default.meter`, `flow.telemetry.default.tracer` — regardless of what is configured under `loggers`/`meters`/`tracers`. Defining your own `default` entry under those keys is allowed and will override the auto-default.

**Options:**

| Option             | Type             | Default | Description                                                                                                  |
|--------------------|------------------|---------|--------------------------------------------------------------------------------------------------------------|
| `framework_logger` | `string \| null` | `null`  | Name of a logger configured under `loggers` (or the always-available `default`) to alias as Symfony `logger` |

**Behavior:**

- When `framework_logger` is set, the bundle aliases the Symfony `logger` service to `flow.telemetry.<framework_logger>.logger.psr3`. If no logger with that name exists, container compilation fails with a clear error.
- When `framework_logger` is `null` and Symfony's `logger` service is the default `Symfony\Component\HttpKernel\Log\Logger`, the bundle automatically aliases `logger` to `flow.telemetry.default.logger.psr3`.
- When `framework_logger` is `null` and `logger` is provided by another bundle (Monolog, custom alias, etc.), the bundle leaves `logger` alone.

```yaml
flow_telemetry:
  loggers:
    app:
      version: '1.0.0'

  framework_logger: app   # Symfony "logger" service -> flow.telemetry.app.logger.psr3
```

## Pattern Matching

Several configuration options support pattern matching for exclusion lists (paths, commands, templates, etc.).

### Exact String Matching

Patterns without regex delimiters match exactly:

```yaml
exclude_paths:
  - path: '/_profiler'    # Matches exactly /_profiler
  - path: '/health'       # Matches exactly /health
```

### Regex Matching

Patterns enclosed in `/` delimiters are treated as regular expressions:

```yaml
exclude_paths:
  - path: '/^\/api\/internal\/.*/'  # Regex: matches /api/internal/*
exclude_commands:
  - '/^debug:.*/'                   # Regex: matches debug:* commands
exclude_templates:
  - '/^@Debug\/.*/'                 # Regex: matches @Debug/* templates
```

## Usage

### Accessing Telemetry in Services

Inject the `Telemetry` service to create custom spans, metrics, and logs:

```php
<?php

namespace App\Service;

use Flow\Telemetry\Telemetry;

final class OrderService
{
    public function __construct(
        private readonly Telemetry $telemetry,
    ) {
    }

    public function processOrder(int $orderId): void
    {
        $tracer = $this->telemetry->tracer('order-service');

        $tracer->trace('process_order', function () use ($orderId) {
            // Your order processing logic
        }, [
            'order.id' => $orderId,
        ]);
    }
}
```

### Creating Custom Spans in Controllers

```php
<?php

namespace App\Controller;

use Flow\Telemetry\Telemetry;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class CheckoutController extends AbstractController
{
    public function __construct(
        private readonly Telemetry $telemetry,
    ) {
    }

    #[Route('/checkout', name: 'checkout')]
    public function checkout(): Response
    {
        $tracer = $this->telemetry->tracer('checkout');

        return $tracer->trace('checkout_page', function () {
            // Nested span for cart validation
            return $this->telemetry->tracer('checkout')->trace(
                'validate_cart',
                fn () => $this->render('checkout/index.html.twig')
            );
        });
    }
}
```

### Recording Metrics

```php
<?php

$meter = $this->telemetry->meter('business-metrics');

// Counter
$meter->counter('orders_processed')
    ->add(1, ['status' => 'completed']);

// Histogram
$meter->histogram('order_value')
    ->record(99.99, ['currency' => 'USD']);

// Gauge
$meter->gauge('active_users')
    ->record(42);
```

### Logging with Telemetry

```php
<?php

$logger = $this->telemetry->logger('app');

$logger->info('Order processed', [
    'order_id' => 12345,
    'amount' => 99.99,
]);
```

## Complete Production Example

```yaml
# config/packages/flow_telemetry.yaml
flow_telemetry:
  resource:
    custom:
      service.name: 'my-app'
      service.version: '%env(APP_VERSION)%'

  propagator:
    type: w3c

  exporters:
    otlp_traces:
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)%'
          timeout: 30
          headers:
            Authorization: 'Bearer %env(OTEL_AUTH_TOKEN)%'
          serializer: { type: protobuf }
    otlp_metrics:
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)%'
          serializer: { type: protobuf }
    otlp_logs:
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)%'
          serializer: { type: protobuf }

  tracer_provider:
    sampler:
      type: trace_id_ratio
      ratio: 0.1  # Sample 10% of traces in production
    processor:
      type: batching
      batch_size: 512
      exporter: otlp_traces

  meter_provider:
    temporality: cumulative
    processor:
      type: batching
      exporter: otlp_metrics

  logger_provider:
    processor:
      type: severity_filtering
      minimum_severity: info
      inner_processor:
        type: batching
        exporter: otlp_logs

  loggers:
    app:
      version: '%env(APP_VERSION)%'
    audit:
      version: '%env(APP_VERSION)%'
      attributes:
        channel: audit
  meters:
    business:
      version: '%env(APP_VERSION)%'
  tracers:
    checkout:
      version: '%env(APP_VERSION)%'

  framework_logger: app   # Symfony "logger" service -> flow.telemetry.app.logger.psr3

  instrumentation:
    http_kernel:
      enabled: true
      exclude_paths:
        - path: '/_profiler'
        - path: '/_wdt'
        - path: '/health'
          method: GET
    console:
      enabled: true
      exclude_commands:
        - 'cache:clear'
        - 'cache:warmup'
        - '/^debug:.*/'
    messenger:
      enabled: true
      context_propagation: true
    dbal:
      enabled: true
      log_sql: true
      max_sql_length: 500
    cache:
      enabled: true
      exclude_pools:
        - 'cache.system'
```
