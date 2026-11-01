---
package: flow-php/symfony-telemetry-bundle
---

# Symfony Telemetry Bundle

The Flow Symfony Telemetry Bundle adds telemetry to Symfony applications. It traces HTTP requests and responses,
instruments console commands, and sends data to OpenTelemetry-compatible backends through configurable exporters.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation
page](/documentation/installation/packages/symfony-telemetry-bundle.md).

## Overview

This bundle is built on [flow-php/telemetry](/documentation/components/libs/telemetry.md). That page describes the
`Telemetry` class, the tracer, meter, and logger API, and the processor and exporter building blocks. To export to
OTLP-compatible backends, the bundle also uses the [Telemetry OTLP
Bridge](/documentation/components/bridges/telemetry-otlp-bridge.md).

The bundle connects the Flow PHP Telemetry library to Symfony applications. It gives you:

- Automatic resource detection: detects the service name, version, environment, OS, host, and process information
- 8 auto-instrumentation options: HTTP kernel, console, messenger, Twig, HTTP client, PSR-18 client, Doctrine DBAL, and
  cache
- Context propagation: W3C TraceContext and Baggage support for distributed tracing
- Configurable exporters: console, memory, void, or OTLP with multiple transport options
- Full Symfony configuration: you configure everything through the Symfony configuration system

## Enabling and Disabling

The bundle is enabled by default. With `enabled: false`, `Flow\Telemetry\Telemetry` stays autowirable, but the bundle
removes everything behind it: exporters, transports, resource detectors, instrumentation, the profiler, and the shutdown
hook.

```yaml
# config/packages/prod/flow_telemetry.yaml
flow_telemetry:
  enabled: false
```

The remaining `Telemetry` uses void processors with sampling off. Spans do not record, logs and metrics go nowhere, and
`flush()` does nothing. Named tracers, meters, loggers, and `#[WithTelemetryChannel]` channel loggers stay registered,
so no injection point breaks. The bundle decorates no Symfony service, and it ignores `framework_logger` and
`capture_framework_channels`. It does not alias `logger` to a void logger, because that discards the application logs.

`flow.telemetry.enabled` is always defined as a container parameter, `true` or `false`.

The bundle reads `enabled` at compile time, so it rejects `%env()%`. A placeholder leaves telemetry wired while you
think that it is off. Use a configuration file for each environment, or a parameter that holds a literal bool.
Environment variables work everywhere else in the tree, also in the sections that disabled mode drops.

For a runtime switch that keeps the services and only stops the export, use `exporters.<name>.enabled`.

## Configuration Reference

> An `error_handler:` field under a provider, processor, or `otlp` exporter refers to a name in the top-level
> `error_handlers:` map. The [Error Handlers](#error-handlers) section describes the map. If you omit the field, it uses
> `default`. If you do not declare that handler, the bundle creates it as `{ type: error_log }`.

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
          path: null     # Cache file path (default: sys_get_temp_dir()/flow_telemetry_resource_<env>.cache)
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

Detector types:

| Type          | Class                       | Category | Attributes Detected                                               |
|---------------|-----------------------------|----------|-------------------------------------------------------------------|
| `os`          | `OsDetector`                | static   | `os.type`, `os.name`, `os.version`, `os.description`              |
| `host`        | `HostDetector`              | static   | `host.name`, `host.arch`, `host.id`                               |
| `service`     | `ComposerDetector`          | static   | `service.name`, `service.version` (from composer.json)            |
| `deployment`  | `SymfonyDeploymentDetector` | static   | `deployment.environment.name` (from kernel environment)           |
| `environment` | `EnvironmentDetector`       | static   | Reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` env vars |
| `process`     | `ProcessDetector`           | dynamic  | `process.pid`, `process.runtime.*`, `process.executable.*`        |

Static detectors are cached by default. Dynamic detectors run on every request and every command.

The cache file is outside the Symfony cache lifecycle on purpose. A Symfony cache build (with `cache:warmup`) at image
build time otherwise freezes runtime attributes, such as `host.name` or `process.pid`, from the build container. The
default location, `sys_get_temp_dir()`, keeps one cache for each runtime and prevents this problem. The default filename
contains the kernel environment (`flow_telemetry_resource_<env>.cache`). Thus, a change of `APP_ENV`, for example from
dev to prod, does not serve an old `deployment.environment.name`. To clear the cached values, delete the cache file or
restart the process. `cache:clear` does not touch the file.

Custom attributes override auto-detected values.

### Clock Configuration

- type: `string|null`
- default: `null`

The service ID of a custom PSR-20 clock. If you do not set it, the bundle uses the built-in SystemClock.

```yaml
flow_telemetry:
  clock_service_id: 'app.clock'
```

### Telemetry Lifecycle

Terminate events are flush points, never shutdown points. On `kernel.terminate` and `console.terminate`, the bundle
flushes buffered signals and pumps async transports. It keeps the transport open, because the unit of work that ends is
not always the last work of the process. A command can run inside another command through `Application::run()`. A
messenger worker continues to consume messages after each handled message. A long-running runtime (FrankenPHP worker
mode, RoadRunner, and others) uses the same kernel for many requests.

The transport shutdown (the final drain and the close) occurs exactly once, at the real end of the process. A shutdown
function does it. The bundle registers this function at the creation of the `Telemetry` service. PHP runs the function
on normal completion, `exit()`, uncaught exceptions, and fatal errors. Thus, in a classic runtime with one process per
request (PHP-FPM, mod_php), it runs at the end of each request. Only a hard kill (SIGKILL, segfault) skips it. In that
case, all data that the bundle flushed at request or message boundaries is already exported. Only the last batch that
was not flushed is lost.

You do not need any configuration. The same behavior is correct in classic runtimes and in worker runtimes.

The bundle also resets the trace context of each request between top-level requests (through `kernel.reset`). Thus, the
context of one worker request never leaks into the next request.

### Context Storage

- type: `enum`
- default: `memory`

The context storage that keeps the trace context across async operations.

```yaml
flow_telemetry:
  context_storage:
    type: memory  # memory|service
    service_id: null  # Custom service ID (only for type: service)
```

### Propagator

- type: `enum`
- default: `w3c`

The context propagator for distributed tracing. It sets how the bundle injects the trace context into HTTP headers and
extracts it from them.

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

### Error Handlers

The OpenTelemetry specification says that the SDK MUST NOT throw to user code at runtime. The bundle catches errors from
exporters and processors and sends them to a configured error handler. The bundle has a named map under
`error_handlers:`. The `error_handler:` fields in provider, processor, and OTLP exporter blocks refer to this map.

```yaml
flow_telemetry:
  error_handlers:
    default:
      type: error_log         # default if omitted

    to_file:
      type: stream
      destination: '%kernel.logs_dir%/flow-telemetry-errors.log'

    to_syslog:
      type: syslog
      facility: local0
      severity: warning

    fanout:
      type: composite
      handlers: [ default, to_file, to_syslog ]

    silent:
      type: noop

    custom:
      type: service
      service_id: app.my_error_handler
```

If `error_handlers:` is omitted, or `default` is missing in it, the bundle adds
`error_handlers.default = { type: error_log }` automatically. Thus, every `error_handler:` reference resolves.

Service IDs registered by the bundle (predictable for `decorates:`):

- `flow.telemetry.error_handler.<name>`: for example, `flow.telemetry.error_handler.default`

#### error_log (default)

Writes formatted Throwables through the PHP `error_log()` function. By default, in CLI, the output goes to stderr.
Otherwise, it goes to the target of the `error_log` ini setting. This follows the recommendation of the OTEL
specification to log to the standard error output.

| Option            | Type    | Default            | Description                                                   |
|-------------------|---------|--------------------|---------------------------------------------------------------|
| `message_type`    | enum    | `operating_system` | `operating_system` (0), `email` (1), `file` (3), `sapi` (4)   |
| `expand_newlines` | boolean | `false`            | Emit one `error_log()` call per line of the formatted message |
| `message_prefix`  | string  | `[flow-telemetry]` | Prefix prepended to every message                             |

#### stream

Appends formatted Throwables (one per line) to a file path or `php://` stream wrapper. The handler opens the handle
lazily on the first call and uses it again for later calls.

| Option               | Type    | Default            | Description                                                                 |
|----------------------|---------|--------------------|-----------------------------------------------------------------------------|
| `destination`        | string  | -                  | File path, `php://stdout`, `php://stderr`, or another PHP stream (required) |
| `file_permissions`   | integer | `0644`             | Permissions for newly created files (ignored for `php://`)                  |
| `create_directories` | boolean | `true`             | Create missing parent directories of the destination                        |
| `message_prefix`     | string  | `[flow-telemetry]` | Prefix prepended to every line                                              |

#### syslog

Writes through `openlog/syslog/closelog`.

| Option     | Type    | Default          | Description                                    |
|------------|---------|------------------|------------------------------------------------|
| `ident`    | string  | `flow-telemetry` | Syslog identity tag                            |
| `facility` | enum    | `user`           | RFC 5424 facility (see table below)            |
| `log_opts` | integer | `LOG_PID`        | Bitmask of `LOG_*` flags passed to `openlog()` |
| `severity` | enum    | `error`          | RFC 5424 severity (see table below)            |

#### udp_syslog

Sends RFC 5424 syslog frames over UDP.

| Option     | Type    | Default          | Description                   |
|------------|---------|------------------|-------------------------------|
| `host`     | string  | -                | Remote syslog host (required) |
| `port`     | integer | `514`            | Remote syslog port            |
| `ident`    | string  | `flow-telemetry` | Syslog identity tag           |
| `facility` | enum    | `user`           | RFC 5424 facility             |
| `severity` | enum    | `error`          | RFC 5424 severity             |

#### composite

Sends an error to multiple named handlers. The handler wraps each child call, so a failing child cannot stop the other
children.

| Option     | Type            | Default | Description                                            |
|------------|-----------------|---------|--------------------------------------------------------|
| `handlers` | list of strings | -       | Names of other entries in `error_handlers:` (required) |

```yaml
fanout:
  type: composite
  handlers: [ default, to_file ]
```

#### noop

Discards every Throwable. Use it for tests or for explicit silence.

```yaml
silent: { type: noop }
```

#### service

Aliases an existing service that implements `Flow\Telemetry\ErrorHandler\ErrorHandler`.

| Option       | Type   | Default | Description                                        |
|--------------|--------|---------|----------------------------------------------------|
| `service_id` | string | -       | Service id of the user-provided handler (required) |

```yaml
custom:
  type: service
  service_id: app.my_error_handler
```

#### Facility values

| Value              | Constant                   |
|--------------------|----------------------------|
| `kernel`           | `LOG_KERN`                 |
| `user`             | `LOG_USER`                 |
| `mail`             | `LOG_MAIL`                 |
| `daemon`           | `LOG_DAEMON`               |
| `auth`             | `LOG_AUTH`                 |
| `syslog`           | `LOG_SYSLOG`               |
| `lpr`              | `LOG_LPR`                  |
| `news`             | `LOG_NEWS`                 |
| `uucp`             | `LOG_UUCP`                 |
| `cron`             | `LOG_CRON`                 |
| `local0`..`local7` | `LOG_LOCAL0`..`LOG_LOCAL7` |

#### Severity values

| Value       | Constant      |
|-------------|---------------|
| `emergency` | `LOG_EMERG`   |
| `alert`     | `LOG_ALERT`   |
| `critical`  | `LOG_CRIT`    |
| `error`     | `LOG_ERR`     |
| `warning`   | `LOG_WARNING` |
| `notice`    | `LOG_NOTICE`  |
| `info`      | `LOG_INFO`    |
| `debug`     | `LOG_DEBUG`   |

### Exporters (named definitions)

The bundle has a top-level named map of exporters. The name of the sub-block under each entry selects the
implementation. There is no separate `type:` key. Each exporter must declare exactly one of the supported sub-blocks:
`otlp`, `service`, `console`, `memory`, `void`. The processors of each signal refer to exporters by name.

```yaml
flow_telemetry:
  exporters:
    otlp: # exporter name
      otlp: # sub-block selects implementation
        transport:
          type: curl
          endpoint: 'http://otel-collector:4318'
          encoding: protobuf
```

| Sub-block | Options                        | Description                               |
|-----------|--------------------------------|-------------------------------------------|
| `otlp`    | `transport: { ... }`           | Sends batches over OTLP curl/grpc/service |
| `service` | `id: <service_id>`             | Aliases an existing user-provided service |
| `console` | none (use `~` / `null` / `{}`) | Pretty-prints to console                  |
| `memory`  | none                           | Stores batches in memory (testing)        |
| `void`    | none                           | Discards everything (no-op)               |

Every exporter also accepts an `enabled` flag (default `true`). If the flag resolves to `false`, the exporter sends
nothing to its backend. The profiler capture and all other exporters continue to work. Thus, telemetry stays visible in
the Symfony profiler, but no data goes to a collector.

The flag accepts a literal boolean or an environment variable:

- A literal `enabled: false` resolves at compile time. It replaces the exporter with a no-op (`void`), and the bundle
  never builds the real backend (for example, the OTLP transport).
- An `enabled: '%env(bool:OTEL_ENABLED)%'` resolves at runtime, for each request. The bundle wraps the real exporter, so
  the environment variable turns the export on or off without a container rebuild. The bundle still builds the backend.
  Thus, a declared `otlp` exporter needs the `flow-php/telemetry-otlp-bridge` package, also while the flag is off.

In both cases, the exporter still declares its real sub-block. To turn the exporter on again, you change only the flag.

```yaml
flow_telemetry:
  exporters:
    otlp:
      enabled: '%env(bool:OTEL_ENABLED)%' # off → discard exports; profiler still captures every signal
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_ENDPOINT)%'
          encoding: protobuf
```

Service IDs registered by the bundle (predictable for `decorates:`):

- `flow.telemetry.exporter.<name>`: for example, `flow.telemetry.exporter.otlp`
- `flow.telemetry.exporter.<name>.transport`: only for an exporter with the `otlp` sub-block

### TracerProvider

Configures the tracer provider for distributed tracing.

```yaml
flow_telemetry:
  tracer_provider:
    error_handler: default  # name from error_handlers; defaults to "default"
    sampler:
      type: parent_based  # always_on|always_off|trace_id_ratio|parent_based|attribute_matching|service (default)
      ratio: 1.0        # Sampling ratio (0.0-1.0, only for trace_id_ratio)
      service_id: null  # Custom sampler service (only for type: service)
      root:
        type: always_on  # parent_based only: always_on|always_off|trace_id_ratio (default)
        ratio: 1.0       # root.type: trace_id_ratio only
    processor:
      type: batching    # composite|memory|batching|passthrough|void|attribute_filtering|service
      batch_size: 512
      exporter: otlp    # name of a top-level exporter
      error_handler: default
```

Sampler types:

| Type                 | Description                                                                                             |
|----------------------|---------------------------------------------------------------------------------------------------------|
| `always_on`          | Sample all traces                                                                                       |
| `always_off`         | Sample no traces                                                                                        |
| `trace_id_ratio`     | Sample based on trace ID ratio                                                                          |
| `parent_based`       | Respect the sampling decision of the parent span. Root spans fall back to `root` (default)              |
| `attribute_matching` | Drop spans that match an attribute matcher (start-time attributes). Pass the rest to a delegate sampler |
| `service`            | Custom sampler service                                                                                  |

### `parent_based` root sampler

`root` selects the sampler for spans that have no parent. These are requests that arrive with no `traceparent` header,
or with a malformed one. It accepts `always_on` (default), `always_off`, and `trace_id_ratio` (with `ratio`).

With `root.type: always_off`, you get the OpenTelemetry `parentbased_always_off` behavior. If an upstream service passes
a sampled `traceparent`, the bundle records the trace. Otherwise, it records nothing. If a proxy such as nginx decides
the sampling, use this configuration:

```yaml
flow_telemetry:
  tracer_provider:
    sampler:
      type: parent_based
      root:
        type: always_off
```

The sampler is global. Under `root.type: always_off`, console commands produce no traces at all. Messenger worker spans
also produce no traces, even for a message from a sampled request. The reason is that a span consumed by a worker starts
its own trace on purpose. This trace links to the producer and does not continue the trace of the producer.

If you want that behavior for HTTP only, leave `root.type` at `always_on` and use
`instrumentation.http_kernel.require_trace_context` instead.

### `attribute_matching` sampler

`attribute_matching` is the OTel way to drop spans by attribute. The decision occurs at span start, so a matched span
never records and never reaches a processor. It sees only the attributes that exist at span start, not end-state values
such as status codes. It uses the same `matcher`, `exclude`, `sources`, and `cache_dir` options as the
[`attribute_filtering`](#attribute_filtering) processor. It also has a `delegate` sampler for spans that do not match:

`delegate` accepts only leaf sampler types (`always_on`, `always_off`, `trace_id_ratio`), not `parent_based`. Thus, an
`attribute_matching` sampler cannot honor upstream sampling decisions for spans that do not match its matcher.

```yaml
flow_telemetry:
  tracer_provider:
    sampler:
      type: attribute_matching
      matcher:
        any:
          - { path: http.route, mode: equal, value: /health }
          - { path: http.route, mode: starts_with, value: /ready }
      # exclude: true (default) drops matches; sources: [signal] (default)
      delegate:
        type: trace_id_ratio   # always_on (default) | always_off | trace_id_ratio
        ratio: 0.1             # everything that is NOT a health/ready probe is 10% sampled
```

### MeterProvider

```yaml
flow_telemetry:
  meter_provider:
    error_handler: default
    temporality: cumulative  # cumulative|delta
    processor:
      type: batching
      batch_size: 512
      exporter: otlp
      error_handler: default
```

### LoggerProvider

```yaml
flow_telemetry:
  logger_provider:
    error_handler: default
    processor:
      type: batching   # composite|memory|batching|passthrough|void|pipeline|service
      batch_size: 512
      exporter: otlp
      error_handler: default
```

Pipeline (logs only). Chain middleware (enrich or filter) before a terminal sink:

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: pipeline
      middleware:
        - { type: severity_filtering, minimum_severity: warn }  # trace|debug|info|warn|error|fatal
      sink:
        type: batching
        exporter: otlp
        batch_size: 200
```

#### Console output (CLI verbosity)

This is the Flow equivalent of the Symfony Monolog `ConsoleHandler`. While a console command runs, the bundle also
prints the emitted log records to the output of that command. The verbosity of the command (`-v`/`-vv`/`-vvv`) filters
these records. This is display only. It does not change what the configured processor exports (for example, OTLP). It is
off by default. As with the MonologBundle console handler, enable it explicitly (usually only in `dev`):

```yaml
# config/packages/flow_telemetry.yaml
when@dev:
  flow_telemetry:
    logger_provider:
      console_output:
        enabled: true
```

The default minimum severity for each verbosity is:

| Verbosity                | Flag   | Minimum severity |
|--------------------------|--------|------------------|
| `VERBOSITY_QUIET`        | `-q`   | `ERROR`          |
| `VERBOSITY_NORMAL`       | (none) | `ERROR`          |
| `VERBOSITY_VERBOSE`      | `-v`   | `WARN`           |
| `VERBOSITY_VERY_VERBOSE` | `-vv`  | `INFO`           |
| `VERBOSITY_DEBUG`        | `-vvv` | `DEBUG`          |

Flow has no `NOTICE` level (the PSR-3 bridge changes `NOTICE` into `INFO`), so there is no step between `WARN` and
`INFO`. The defaults keep the terminal quiet (errors only) and show exactly one more severity for each `-v` step.

`-vvv` is the most verbose flag in Symfony, and the default map stops at `DEBUG`. Thus, with the defaults, the console
never shows `TRACE` logs. They still reach your exporters, for example OTLP. To see `TRACE` on the console, map a flag
to it with `verbosity_levels`. The key is the name of the Symfony verbosity constant. The value is a Flow severity name
(`TRACE|DEBUG|INFO|WARN|ERROR|FATAL`). For example, point `-vvv` at `TRACE`:

```yaml
flow_telemetry:
  logger_provider:
    console_output:
      enabled: true
      verbosity_levels:
        VERBOSITY_DEBUG: TRACE   # -vvv now shows TRACE and everything above it
```

You can override any step in the same way. For example, show `INFO` and above with no flag at all:

```yaml
flow_telemetry:
  logger_provider:
    console_output:
      enabled: true
      verbosity_levels:
        VERBOSITY_NORMAL: INFO   # show INFO and above without any -v flag
        VERBOSITY_VERBOSE: DEBUG
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

Exports each item immediately through the referenced exporter.

```yaml
processor:
  type: passthrough
  exporter: console
```

#### memory

Stores items in memory (for testing). It still needs a backing exporter for `flush()` to call.

```yaml
processor:
  type: memory
  exporter: memory
```

#### batching

Batches items before export.

| Option          | Type    | Default | Description                                                                                                                         |
|-----------------|---------|---------|-------------------------------------------------------------------------------------------------------------------------------------|
| `batch_size`    | integer | `512`   | Number of items per batch                                                                                                           |
| `max_batch_age` | float   | `null`  | Maximum seconds before the processor exports a partial batch, from the first buffered signal. `null` turns off the time-based flush |
| `exporter`      | string  | -       | Name of a top-level exporter (required)                                                                                             |

```yaml
processor:
  type: batching
  batch_size: 512
  max_batch_age: 15.0   # export at least every 15s in long-running processes
  exporter: otlp
```

By default, the processor exports a batch only in three cases: the batch reaches `batch_size`, a call to
`flush()`/`shutdown()` occurs, or the process exits. In a request-scoped app, this is fine, because the buffer drains at
the end of the request. In a long-running process, a low-rate signal can stay in the buffer until 512 signals collect or
the process stops. Examples are Symfony Messenger workers, daemons, and persistent runtimes such as ReactPHP,
RoadRunner, Swoole, AMP, or FrankenPHP worker mode. To limit how long one signal waits before export, set
`max_batch_age` (for example, `15.0`). For request-scoped apps, keep it unset, and the behavior stays the same.

> PHP limitation for idle processes. PHP has no background timer thread. Thus, the processor evaluates the age deadline
> only at the processing of a signal, not on a wall-clock schedule. A fully idle process ends no spans and processes no
> metrics or logs. Such a process cannot flush a partial batch itself on the age trigger. In this case, an external
> flush is necessary: the Messenger worker-event flush subscriber, a runtime loop that calls `Telemetry::flush()`, or
> `shutdown()` at process exit. The deadline uses a monotonic clock (`hrtime`), so changes to the wall clock do not
> affect it.

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

#### pipeline (logger_provider only)

Runs each log record through an ordered list of middleware (enrich or filter). Then it sends the remaining records to a
single sink. This is the way to combine more than one processing step for logs. It follows the OpenTelemetry
`LogRecordProcessor` model. Each step can change the record or drop it, and the next step sees the changes.

| Option       | Type   | Default  | Description                                                                                                        |
|--------------|--------|----------|--------------------------------------------------------------------------------------------------------------------|
| `middleware` | list   | `[]`     | Ordered middleware. The first middleware that drops a record skips the rest                                        |
| `sink`       | object | required | Terminal processor (`memory`, `batching`, `passthrough`, `void`, `service`, or `composite`) that exports survivors |

Each `middleware` entry has a `type` and its own options:

| `type`                | Options                                                                                        | Effect                                           |
|-----------------------|------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `enriching`           | `attributes` (map)                                                                             | Merges default attributes (call-site values win) |
| `severity_filtering`  | `minimum_severity` (`trace`...`fatal`, default `info`)                                         | Drops records below the level                    |
| `attribute_filtering` | `matcher`, `exclude`, `sources`, `cache_dir` (see [attribute_filtering](#attribute_filtering)) | Drops/keeps records by attribute matcher         |

```yaml
processor:
  type: pipeline
  middleware:
    - { type: enriching, attributes: { 'deployment.environment': prod } }
    - { type: severity_filtering, minimum_severity: warn }
    - type: attribute_filtering
      matcher: { any: [ { path: http.route, mode: equal, value: /health } ] }
  sink:
    type: batching
    exporter: otlp
    batch_size: 200
```

#### attribute_filtering

Drops (or keeps) signals by their attributes. Use it to reduce noise, for example to discard health-check or bot traffic
before export. On `tracer_provider` and `meter_provider`, it is a processor that wraps an `inner_processor`. The inner
processor receives the remaining signals. On `logger_provider`, it is a middleware in a
[`pipeline`](#pipeline-logger_provider-only) instead. There is no `inner_processor`, and the `sink` of the pipeline
receives the remaining records. The `matcher`/`exclude`/`sources`/`cache_dir` options below apply in both forms.

| Option                  | Type         | Default                                     | Description                                                                                                                                                                                                                                                                  |
|-------------------------|--------------|---------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `matcher`               | object       | required                                    | The matcher tree (see below)                                                                                                                                                                                                                                                 |
| `exclude`               | boolean      | `true`                                      | `true`: drop matching signals. `false`: keep ONLY matching signals                                                                                                                                                                                                           |
| `sources`               | list of enum | `[signal]`                                  | The attribute sets to inspect: any of `signal`, `resource`, `scope`. The bundle evaluates the matcher against each listed source and combines the results with OR. A match in any source is a match for the signal                                                           |
| `cache_dir`             | string       | `%kernel.cache_dir%/flow_telemetry_filters` | Directory for the compiled matcher cache                                                                                                                                                                                                                                     |
| `cache_dir_permissions` | int          | `0o700`                                     | Octal mode for a new cache directory (owner-only by default, because it holds `require`d PHP). The umask also applies. Write it as a YAML octal literal, for example `0o750`. It applies only at creation. The bundle does not change an existing directory                  |
| `inner_processor`       | object       | required for span/metric                    | Tracer and meter only. The wrapped processor that receives the remaining signals (leaf types only: `memory`, `batching`, `passthrough`, `void`, `service`). For logs, omit it. The `sink` of the [`pipeline`](#pipeline-logger_provider-only) receives the remaining records |

The `matcher` is a tree. Each node is exactly one of:

- `all: [ ... ]`: a list of child matchers. All children must match (AND).
- `any: [ ... ]`: a list of child matchers. At least one child must match (OR).
- `not: { ... }`: a single child matcher. The child must not match.
- a leaf rule with a `path`. The four kinds are mutually exclusive. A mix of them in one node is a configuration error.

Composites and leaves nest to any depth, so you can express any combination of AND, OR, and NOT. A leaf rule accepts:

| Option           | Type         | Default  | Description                                                              |
|------------------|--------------|----------|--------------------------------------------------------------------------|
| `path`           | string\|list | required | Attribute key, or a list of segments descending into nested array values |
| `mode`           | enum         | required | Comparison mode (see below)                                              |
| `value`          | scalar       | required | Expected value (must be a string for the pattern modes)                  |
| `case_sensitive` | boolean      | `true`   | Substring modes only                                                     |

Modes: `equal`, `not_equal`, `greater_than`, `greater_than_equal`, `less_than`, `less_than_equal`, `regexp`,
`starts_with`, `ends_with`, `contains`.

Drop health-check and bot logs. A match in any branch drops the log. On `logger_provider`, attribute filtering is a
middleware in a [`pipeline`](#pipeline-logger_provider-only), and the `sink` receives the remaining records:

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: pipeline
      middleware:
        - type: attribute_filtering
          matcher:
            any:
              - { path: http.route, mode: equal, value: /health }
              - { path: http.user_agent, mode: contains, value: bot, case_sensitive: false }
      sink:
        type: batching
        exporter: otlp
```

Keep only payments-scope metrics that are not internal (`exclude: false`, `sources: [scope]`, negation):

```yaml
flow_telemetry:
  meter_provider:
    processor:
      type: attribute_filtering
      exclude: false
      sources: [ scope ]
      matcher:
        all:
          - { path: name, mode: equal, value: app.payments }
          - not: { path: internal, mode: equal, value: true }
      inner_processor:
        type: batching
        exporter: otlp
```

Inspect more than one source at once. Drop a signal with the route `/health`, on the signal itself or on the resource:

```yaml
flow_telemetry:
  tracer_provider:
    processor:
      type: attribute_filtering
      sources: [ signal, resource ]
      matcher: { path: http.route, mode: equal, value: /health }
      inner_processor:
        type: batching
        exporter: otlp
```

Deeply nested combinations. Drop a span that is a fast internal call, or a non-health route on a flagged tenant
(`(duration < 5 AND internal) OR (tenant = a AND NOT route ^= /health)`):

```yaml
flow_telemetry:
  tracer_provider:
    processor:
      type: attribute_filtering
      matcher:
        any:
          - all:
              - { path: [ timing, duration_ms ], mode: less_than, value: 5 }
              - { path: internal, mode: equal, value: true }
          - all:
              - { path: tenant, mode: equal, value: a }
              - not: { path: http.route, mode: starts_with, value: /health }
      inner_processor:
        type: batching
        exporter: otlp
```

Filter logs by severity (logs only). Severity is not an attribute. But on `logger_provider`, the `attribute_filtering`
middleware gives the severity of the record to the matcher as two reserved signal keys. Thus, you can express severity
thresholds per channel (or per attribute) together with any other attribute:

- `log.severity`: the OpenTelemetry severity number (`trace`=1, `debug`=5, `info`=9, `warn`=13, `error`=17,
  `fatal`=21), for ordered comparisons (`greater_than_equal`, ...).
- `log.severity_name`: the level name (`TRACE`...`FATAL`), for `equal`/`regexp`.

These keys exist only for the filter decision. The bundle never exports them, because the severity already goes in the
native OTLP `severityNumber`/`severityText` fields. They hide any user attribute with the same name. They are on the
`signal` source. Thus, an `all` node that combines severity with another attribute needs that attribute on `signal` too.
Under the default `channel_attribute_target: both`, the `flow.log.channel` attribute of the channel is on `signal`.

Keep `error`+ from the `payments` channel, but `debug`+ from the `importer` channel. With `exclude: false`, only
matching records stay. One global [`severity_filtering`](#pipeline-logger_provider-only) threshold cannot express this
case:

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: pipeline
      middleware:
        - type: attribute_filtering
          exclude: false
          matcher:
            any:
              - all:
                  - { path: flow.log.channel, mode: equal, value: payments }
                  - { path: log.severity, mode: greater_than_equal, value: 17 }   # error+
              - all:
                  - { path: flow.log.channel, mode: equal, value: importer }
                  - { path: log.severity, mode: greater_than_equal, value: 5 }    # debug+
      sink:
        type: batching
        exporter: otlp
```

For one global threshold, use the simpler `severity_filtering` middleware. Use `log.severity` only for a threshold that
changes by channel or by another attribute.

The bundle compiles the matcher to a cached PHP matcher in `cache_dir` (the project cache directory by default). Thus,
the evaluation of each signal stays cheap. If the directory is not writable, the bundle uses interpreted matching. The
bundle loads the compiled file with `require`, so `cache_dir` must be trusted, that is, not writable by untrusted users.
The project cache directory is private to the application, so the default is safe.

### Exporter Definitions

You declare exporters once at the top level under `exporters:`. Processor blocks refer to them by name. Each exporter
declares exactly one sub-block, and the name of the sub-block selects the implementation.

#### void

Discards all data.

```yaml
exporters:
  drop: { void: ~ }
```

#### memory

In-memory store for testing. The service has the `allLogs()`, `allMetrics()`, and `allSpans()` accessors through
`Flow\Telemetry\Provider\Memory\MemoryExporter`.

```yaml
exporters:
  capture: { memory: ~ }
```

#### console

Pretty-prints logs, metrics, and spans to the console. Use it for development.

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
      error_handler: default   # name from error_handlers; defaults to "default"
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        encoding: protobuf
```

#### service

Aliases an existing user-defined service that implements `Flow\Telemetry\Exporter\Exporter`. Use it for APM-specific
exporters (Datadog, New Relic, custom) that do not use OTLP. Define your exporter as a Symfony service in your own
`services.yaml` and refer to it by id.

```yaml
exporters:
  datadog:
    service:
      id: 'app.datadog_telemetry_exporter'
```

### OTLP Transport Configuration

Inside `exporters.<name>.otlp.transport`. Required for the `otlp` sub-block.

#### Transport type

| `type`       | Transport            | Notes                                                                                                                                                              |
|--------------|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `curl`       | `CurlTransport`      | Synchronous HTTP (default). Each `send()` blocks until the response.                                                                                               |
| `async_curl` | `AsyncCurlTransport` | Non-blocking `curl_multi`. With `messenger` instrumentation enabled, the bundle pumps it on the Messenger `WorkerRunningEvent`. Otherwise, pump `tick()` yourself. |
| `grpc`       | `GrpcTransport`      | OTLP/gRPC (Protobuf only).                                                                                                                                         |
| `stream`     | `StreamTransport`    | JSONL to a file path or `php://` stream.                                                                                                                           |
| `service`    | user service         | Aliases an existing transport service id.                                                                                                                          |

Use `curl` by default. Use `async_curl` only for non-blocking dispatch. In a Messenger worker, the bundle pumps it on
`WorkerRunningEvent`. In other processes, you must call `tick()` yourself. The [OTLP bridge async transport
section](/documentation/components/bridges/telemetry-otlp-bridge.md#async-curl-transport) has more information.

#### Timeouts

The defaults expect the recommended deployment: an OpenTelemetry Collector that runs close to the application (loopback,
UDS, or sidecar):

| Setting               |                                 Default | Applies to             | Bounds                                                      |
|-----------------------|----------------------------------------:|------------------------|-------------------------------------------------------------|
| `timeout_ms`          | 10000 curl / 5000 async_curl / 250 grpc | curl, async_curl, grpc | Per-request deadline (curl: total request, grpc: per-call)  |
| `connect_timeout_ms`  |              250 curl / 1500 async_curl | curl, async_curl       | TCP/TLS connect. gRPC has no separate bound                 |
| `pump_timeout_ms`     |                                     100 | async_curl only        | Per-`tick()` bounded drive budget (`0` = single exec round) |
| `shutdown_timeout_ms` |                                    5000 | curl, async_curl, grpc | Wall-clock budget for draining pending requests at shutdown |

`curl` is synchronous, so its `timeout_ms` is the maximum time for each flush. `async_curl` uses a larger 1500 ms
`connect_timeout_ms`, because it moves forward only at a pump. It also uses a 100 ms `pump_timeout_ms` for each
`tick()`. gRPC progresses in the background, so its deadline for each call stays at 250 ms. `shutdown_timeout_ms` limits
the graceful drain at exit. The [OTLP bridge Timeouts
section](/documentation/components/bridges/telemetry-otlp-bridge.md#timeouts) has more information.

#### Failover Transport

The `curl`, `async_curl` and `grpc` transports accept a nested `failover:` block. If the primary delivery fails, the transport sends
the batch to the failover transport. It also raises a `FailoverTransportException`, so the operator knows about the
failure. This is also true for data that the failover keeps. A common pattern is a gRPC primary with a stream (JSONL on
disk) failover. Then a collector that is down still leaves data that the operator can replay later.

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: grpc
        endpoint: 'otel-collector:4317'
        timeout_ms: 250
        failover:
          type: stream
          endpoint: '%kernel.logs_dir%/otel-failed.jsonl'
```

Constraints

- The `failover:` block accepts the same fields as the parent transport. It cannot declare a nested `failover:` itself
  (single-level depth).
- Only `curl`, `async_curl`, and `grpc` primaries allow it. The configuration validation rejects `failover` under a
  `stream` or `service` primary.
- The bundle registers `flow.telemetry.exporter.<name>.failover.transport` for the failover service id.

The [OTLP bridge Failover Transport
section](/documentation/components/bridges/telemetry-otlp-bridge.md#failover-transport) describes the underlying
behavior. It covers the rules for a forwarded batch (absorbed or lost), the shape of `FailoverTransportException`, and
the cascade-shutdown contract.

#### curl (default)

| Option                | Type    | Default | Description                                                                 |
|-----------------------|---------|---------|-----------------------------------------------------------------------------|
| `endpoint`            | string  | -       | OTLP base URL (required)                                                    |
| `timeout_ms`          | integer | `10000` | Total per-request deadline in milliseconds                                  |
| `connect_timeout_ms`  | integer | `250`   | TCP/TLS connect deadline in milliseconds                                    |
| `shutdown_timeout_ms` | integer | `5000`  | Wall-clock budget in milliseconds for draining pending requests at shutdown |
| `compression`         | boolean | `false` | Enable compression                                                          |
| `follow_redirects`    | boolean | `true`  | Follow HTTP redirects                                                       |
| `max_redirects`       | integer | `3`     | Maximum redirects to follow                                                 |
| `proxy`               | string  | `null`  | Proxy URL                                                                   |
| `ssl_verify_peer`     | boolean | `true`  | SSL peer verification                                                       |
| `ssl_verify_host`     | boolean | `true`  | SSL host verification                                                       |
| `ssl_cert_path`       | string  | `null`  | SSL certificate path                                                        |
| `ssl_key_path`        | string  | `null`  | SSL key path                                                                |
| `ca_info_path`        | string  | `null`  | CA info path                                                                |
| `headers`             | object  | `{}`    | Additional HTTP headers                                                     |
| `encoding`            | enum    | `json`  | OTLP/HTTP wire encoding: `json` or `protobuf`                               |
| `failover`            | object  | `null`  | Optional [failover transport](#failover-transport)                          |

See [Timeouts](#timeouts) for guidance on the millisecond defaults.

```yaml
exporters:
  otlp:
    otlp:
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        timeout_ms: 250
        connect_timeout_ms: 250
        compression: true
        headers:
          Authorization: 'Bearer token'
        encoding: protobuf
```

#### grpc

gRPC transport. The validation rejects `encoding`, because OTLP/gRPC requires Protobuf, which the transport builds
internally. It also rejects `connect_timeout_ms`, because gRPC has no separate connect bound. `timeout_ms` is the
deadline for each call, and it covers DNS, connect, send, and receive together.

| Option                | Type    | Default | Description                                                              |
|-----------------------|---------|---------|--------------------------------------------------------------------------|
| `endpoint`            | string  | -       | gRPC endpoint (required)                                                 |
| `timeout_ms`          | integer | `250`   | Per-call deadline in milliseconds                                        |
| `shutdown_timeout_ms` | integer | `5000`  | Wall-clock budget in milliseconds for draining pending calls at shutdown |
| `insecure`            | boolean | `true`  | Allow insecure connections                                               |
| `headers`             | object  | `{}`    | gRPC metadata                                                            |
| `failover`            | object  | `null`  | Optional [failover transport](#failover-transport)                       |

```yaml
exporters:
  otlp_grpc:
    otlp:
      transport:
        type: grpc
        endpoint: 'otel-collector:4317'
        timeout_ms: 250
        insecure: false
```

#### stream

OTLP File Exporter ([spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/)). Writes one JSON Line for
each batch to the configured destination, with `LOCK_EX` around each `fwrite`. The destination is an absolute file path
or a `php://` stream wrapper. The specification allows only JSON encoding. At configuration time, the bundle rejects
`encoding` and the HTTP-specific options (`timeout`, `ssl_*`, `headers`, and others).

| Option               | Type    | Default | Description                                                                             |
|----------------------|---------|---------|-----------------------------------------------------------------------------------------|
| `endpoint`           | string  | -       | File path or `php://` stream wrapper URI (required)                                     |
| `file_permissions`   | integer | `0644`  | File mode for new files. Ignored for `php://` destinations                              |
| `create_directories` | boolean | `true`  | Create missing parent directories of the destination. Ignored for `php://` destinations |

```yaml
exporters:
  otlp_logs_file:
    otlp:
      transport:
        type: stream
        endpoint: '%kernel.project_dir%/var/otel/logs.jsonl'
        file_permissions: 0640
        create_directories: true

  otlp_logs_stdout:
    otlp:
      transport:
        type: stream
        endpoint: 'php://stdout'
```

To write all three signal types to one file, point three exporters at the same destination. The `otlpjsonfile` receiver
of the OpenTelemetry Collector reads mixed `resourceLogs` / `resourceMetrics` / `resourceSpans` lines.

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

### Multiple OTLP backends

Each signal can send to its own collector. Declare multiple named exporters and refer to them in each provider.

```yaml
flow_telemetry:
  exporters:
    otlp_traces:
      otlp:
        transport:
          type: grpc
          endpoint: 'http://traces:4317'
          insecure: false
    otlp_metrics:
      otlp:
        transport:
          type: curl
          endpoint: 'http://metrics:4318'
          encoding: protobuf
    otlp_logs:
      otlp:
        transport:
          type: curl
          endpoint: 'http://logs:4318'
          encoding: json

  tracer_provider:
    processor: { type: batching, exporter: otlp_traces,  batch_size: 1024 }
  meter_provider:
    processor: { type: batching, exporter: otlp_metrics, batch_size: 256 }
  logger_provider:
    processor: { type: batching, exporter: otlp_logs,    batch_size: 100 }
```

### Migrating from older config

The schema replaced the `type: <name>` discriminator with a sub-block. The key of the sub-block names the
implementation. There is no BC shim.

Breaking change: the default `sampler.type` changed from `always_on` to `parent_based`. The
[TracerProvider](#tracerprovider) section above describes it. By default, the bundle now drops spans whose parent has an
unsampled `traceparent`. To get the previous behavior back, set `sampler.type: always_on` explicitly.

Before (legacy schema)

```yaml
flow_telemetry:
  exporters:
    otlp:
      type: otlp
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        encoding: protobuf
    custom:
      type: service
      service_id: 'app.x'
    debug: { type: console }
```

After

```yaml
flow_telemetry:
  exporters:
    otlp:
      otlp:
        transport:
          type: curl
          endpoint: 'http://otel-collector:4318'
          encoding: protobuf
    custom:
      service:
        id: 'app.x'
    debug: { console: ~ }

  tracer_provider:
    processor: { type: batching, exporter: otlp }
```

### Instrumentation

Configure automatic instrumentation for Symfony components.

All instrumentation is disabled by default. You must explicitly set `enabled: true` for each component you want to
instrument.

#### HTTP Kernel

Traces HTTP requests and responses.

```yaml
flow_telemetry:
  instrumentation:
    http_kernel:
      enabled: true
      context_propagation: true  # Extract context from incoming headers
      context_propagation_query: false  # Also extract from the URL query string (off by default; see note below)
      require_trace_context: false  # Suppress requests without a trace context (off by default; see note below)
      route_naming: path  # Span name / http.route source: 'path' (template, e.g. /orders/{id}; default) or 'name'
      trace_controller: true                  # Controller body span (default ON)
      trace_controller_resolution: false      # controller.get_callable span (default OFF)
      trace_controller_arguments: false       # controller.get_arguments aggregate span (default OFF)
      trace_controller_argument_resolvers: false  # per-resolver controller.argument_value_resolver spans (default OFF)
      exclude_paths:
        - path: '/_profiler'
        - path: '/_wdt'
        - path: '/health'
          method: GET
        - path: '/^\/api\/internal\/.*/'  # Regex pattern
```

An excluded path suppresses tracing for the whole request, not only for its request span. The bundle attaches the
OpenTelemetry suppression key for the duration of the request (through `kernel.terminate`). Thus, lower-level
auto-instrumentation (DBAL, cache) and all work in `kernel.terminate` listeners produce no spans. This stops excluded
requests from emitting orphan root spans for database writes after their response. An example of an excluded request is
the `/_wdt` toolbar fetch on every dev page.

The request (SERVER) span follows the OpenTelemetry HTTP semantic conventions for its name and `http.route`.
`route_naming` controls them:

- `path` (default): the route path template, for example `GET /orders/{id}` (low cardinality, the semconv value for
  `http.route`). The bundle reads it from a `[route name => path]` map. An optional cache warmer (`cache:warmup`) builds
  this map once per deployment. If the map is missing, the bundle builds it lazily. Thus, the request path never queries
  the router.
- `name`: the Symfony route name, for example `GET order_show`.
- Sub-requests (`render(controller(...))`) have no route, so the bundle names them after the controller
  (`GET App\Controller\NavigationController::top`). A request that matches no route uses only the method (`GET`).

The bundle can also trace the controller lifecycle as child spans of the request span (same instrumentation scope, kind
`INTERNAL`). It emits them only while the request span exists. Thus, with `http_kernel` disabled or the path excluded,
there are none.

- `trace_controller` (default true): the execution of the controller body. The span has the name of the resolved
  controller (for example, `App\Controller\OrderController::import`). It has the `code.function.name` and
  `flow.symfony.controller` attributes. It starts after argument resolution and completes at
  `kernel.view`/`kernel.response`, so it excludes the resolution time. It appears in the OTLP export and in the Flow
  Telemetry profiler panel.
- `trace_controller_resolution` (default false): controller resolution
  (`ControllerResolverInterface::getController()`), emitted as a `controller.get_callable` span.
- `trace_controller_arguments` (default false): argument resolution as a single aggregate
  `controller.get_arguments` span.
- `trace_controller_argument_resolvers` (default false): one `controller.argument_value_resolver` span per value
  resolver invocation (finer-grained, higher cardinality).

If the resolution and argument toggles are off, they install no service decorators and add zero overhead.

`require_trace_context` suppresses any request that arrives without a trace context, including its DBAL and cache spans.
It applies only to HTTP. Console commands and messenger workers are unaffected. It needs `context_propagation: true` and
the `flow-php/symfony-http-foundation-telemetry-bridge` package. Without either of them, the bundle silently ignores the
flag and traces requests as though it is unset.

If a proxy such as nginx decides sampling for inbound traffic, but you still want traces for cron jobs and workers, use
`require_trace_context: true`. To apply the "require an upstream decision" rule to every entry point in the process, use
`sampler.root.type: always_off`. The flag looks only at whether a trace context is present, not at whether it was
sampled. Thus, it does not honor an upstream `sampled=0` decision on its own. The default of the `parent_based` sampler
does that. With `sampler.type: always_on` and this flag, the bundle traces again the requests that nginx explicitly
declined to sample.

#### Security

Decorates the request span with the authenticated user. Requires `symfony/security-core` (and
`symfony/security-http` to capture logins that happen during the request).

```yaml
flow_telemetry:
  instrumentation:
    security:
      enabled: true
      fields:
        id:
          enabled: true        # getUserIdentifier() (default ON)
          attribute: user.id
        roles:
          enabled: false       # getRoleNames() (default OFF)
          attribute: user.roles
        email:
          enabled: false       # read from a getter on the user object (default OFF)
          attribute: user.email
          getter: getEmail
```

The bundle reads the user from the token storage at `kernel.controller` and again on `LoginSuccessEvent`. Thus, it
covers requests that are already authenticated and logins in the middle of a request. Each field is independent, and you
can configure its attribute key. The bundle reads `email` from the configured getter. If the method is missing or
returns a non-scalar, the bundle skips the field. The bundle does not change anonymous requests.

The bundle reads the token from `security.untracked_token_storage`, and only for a request that has a span. Thus, the
instrumentation never marks the session as used. Behind a lazy firewall, it never turns a cacheable response into
`Cache-Control: private`.

To attach application-specific attributes, implement `UserSpanAttributeProvider` and tag the service. The tag is
autoconfigured. The bundle merges the returned attributes onto the request span. On a key collision, the returned
attributes win:

```php
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserSpanAttributeProvider;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

final class TenantAttributes implements UserSpanAttributeProvider
{
    public function attributes(TokenInterface $token): array
    {
        $user = $token->getUser();

        return $user instanceof AppUser ? ['app.tenant_id' => $user->getTenantId()] : [];
    }
}
```

#### Console commands

Traces console commands.

```yaml
flow_telemetry:
  instrumentation:
    console:
      enabled: true
      exclude_commands: # default: ['messenger:consume']
        - 'messenger:consume'
        - 'app:my-worker'
        - '/^debug:.*/'   # Regex: exclude all debug commands
```

`exclude_commands` fully suppresses the listed commands. The bundle drops the command and every span under it, not only
the span of the command. Patterns match the command name exactly or as a regex (with `/` delimiters). The option applies
also with console spans disabled, so worker suppression works with `console.enabled: false`.

This does not affect instrumentation that starts its own root trace. An example is the messenger handler for each
message, which the middleware records as a separate trace. Thus, the exclusion of `messenger:consume` (the default)
silences the worker loop (transport poll, idle-tick co-listeners). The bundle still traces each handled message.

The default is `['messenger:consume']`. Set `exclude_commands: []` to trace everything, including the messenger worker
loop.

#### Messenger

Traces Symfony Messenger messages with context propagation across message boundaries.

```yaml
flow_telemetry:
  instrumentation:
    messenger:
      enabled: true
      context_propagation: true    # Propagate context across message boundaries
      trace: true                  # Emit a per-message span (default true)
      metrics: true                # Emit messaging metrics (default true)
      span_naming: transport       # Span name suffix: 'transport' (default), 'message_name' or 'message_fqcn'
```

The bundle adds the tracing middleware to every message bus automatically. You do not need a manual
`framework.messenger` middleware configuration.

`trace: true` (default) emits one span per message: `process` for consumed messages, `send` for produced messages.
`span_naming` controls the part of the span name after the operation:

- `transport` (default): the OTEL semconv destination, for example `process async` / `send async`. At the start of a
  dispatch, the transport is unknown, because routing occurs deeper in the middleware stack. Thus, the bundle renames
  the producer span after it sends the message. A message routed to multiple transports (or handled in-process) keeps
  the name with only the operation (`send`).
- `message_name`: the short class name of the message, for example `send CreateOrderMessage` /
  `process CreateOrderMessage`. It is different from the OTEL messaging semconv pattern (`{operation} {destination}`),
  but it gives much more information. The official OTel Symfony contrib instrumentation names its messenger spans in the
  same way.
- `message_fqcn`: the fully qualified class name, for example `send App\Message\CreateOrderMessage`.

In all naming modes, the transport stays on the `messaging.destination.name` attribute, and the message class stays on
`flow.messenger.message.class`. The bundle suppresses the whole `messenger:consume` command through
[`console.exclude_commands`](#console-commands) (default `['messenger:consume']`). Thus, the work of the worker between messages
(transport poll, idle-tick co-listeners) does not appear. The bundle sets a suppression flag on the telemetry context at
the start of the command, and clears it at the end. Suppression applies only to tracing (as in OpenTelemetry), and a
sampler enforces it. Metrics and logs are unaffected. `TracingMiddleware` lifts the suppression for each message, so the
bundle traces the `process` span and all that it calls as usual. The command is suppressed, so the worker does not emit
the long-lived `messenger:consume` console span. The messenger metrics show that the worker is alive.

`trace: false` keeps the worker fully suppressed and emits no per-message spans. With `metrics: true`, metrics still
emit.

When `context_propagation` is on, the `process` span of a consumed message links back to the producing span. The
telemetry stamp of the message carries the link.

A message that a worker consumes is the root of its own trace, as the OpenTelemetry messaging conventions say. The
producer and the consumer get separate traces, and a span link connects them. Thus, a long-running worker never puts all
messages into one trace. Also, the queue wait time never becomes part of the duration of a span. A synchronously
received message (the `sync://` transport) is handled in the current request or command. Thus, its `process` span stays
a child of the current trace.

When handling fails, the `error.type` and the recorded exception of the span are the actual handler failures. They are
not the `HandlerFailedException` wrapper that Messenger throws.

The bundle flushes buffered telemetry after each handled or failed message, while the worker continues to run. Thus,
consumer traces, logs, and metrics export quickly, not only at the stop of the worker. Failures also flush. Thus, error
spans and exception logs are visible, also for a message that is retried or sent to the failure transport.

A flush per message means one exporter round-trip per message. For high-throughput workers, set
[`max_batch_age`](#batching) on the batching processor. Then the batch collects signals from many messages, and an idle
worker still exports on a time limit, not per message.

The OTLP `curl` transport sends synchronously. Thus, the flush for each message exports the batch and reports its result
before the handler returns. There is nothing to pump between messages, and no request stays in flight until shutdown.
Each flush blocks for up to the curl `timeout_ms`. To keep that time below one millisecond, keep a Collector close to
the worker (loopback/UDS/sidecar). The [Timeouts](#timeouts) section has more information.

With the `async_curl` transport, the subscriber also pumps the `tick()` of each transport on every `WorkerRunningEvent`.
Thus, in-flight requests complete in the background.

Metrics.

| Metric                               | Instrument | Unit        | Emitted                                                                              | Attributes                                                                                                                           |
|--------------------------------------|------------|-------------|--------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `messaging.client.consumed.messages` | Counter    | `{message}` | once per received message                                                            | `messaging.system`, `messaging.operation.name=process`, `messaging.destination.name` (transport), `error.type` on a handling failure |
| `messaging.process.duration`         | Histogram  | `s`         | per received message                                                                 | same as above                                                                                                                        |
| `messaging.client.sent.messages`     | Counter    | `{message}` | once per message sent to a transport (in-process handled dispatches are not counted) | `messaging.system`, `messaging.operation.name=send`, `messaging.destination.name` (single transport), `error.type` on a send failure |

`messaging.process.duration` is recorded in seconds with the OTEL-recommended bucket boundaries.

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

`url.full`, `url.scheme`, `server.address`, and `server.port` describe the effective URL that the client sends
(`$response->getInfo('url')`). Thus, they include `base_uri` and the `query` option.

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

#### Sensitive query parameters

These instrumentations replace the userinfo and the values of listed query parameters with `REDACTED`: `http_kernel`
(`url.query`), `http_client`, and `psr18_client` (`url.full`, `exception.message`). The default is
`UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS`. It holds OpenTelemetry, OAuth 2.0 / OpenID Connect, API key, and
credential keys. The [URL sanitization](/documentation/components/libs/telemetry.md#url-sanitization) section lists
them.

A configured list replaces the default. Keys are case-sensitive. To add keys, extend the constant in a PHP configuration
file:

```php
// config/packages/flow_telemetry.php
use Flow\Telemetry\Sanitizer\UrlSanitizer;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container): void {
    $container->extension('flow_telemetry', [
        'instrumentation' => [
            'sanitization' => [
                'url' => [
                    'sensitive_query_parameters' => [...UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS, 'fb_exchange_token'],
                ],
            ],
        ],
    ]);
};
```

#### Doctrine DBAL

Traces database queries.

```yaml
flow_telemetry:
  instrumentation:
    dbal:
      enabled: true
      max_sql_length: 1000          # Max db.query.text length (0 = no limit)
      collect_metrics: true         # Emit db.client.operation.duration + db.client.response.returned_rows
      include_parameters: false     # Include bound statement parameters (security consideration)
      max_parameters: 10            # Max parameters to include when include_parameters is enabled
      max_parameter_length: 100     # Max length per included parameter value
      transaction_spans: grouped    # grouped (one span per transaction) | per_operation | off
      exclude_connections:
        - 'legacy'
        - '/^test_.*/'
      exclude_tables: # Queries referencing these tables are not traced
        - 'cache_items'      
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
      flush_deferred: false   # opt-in; see below
```

The bundle traces every service with the `cache.pool` tag. This includes the internal pools of the framework
(`cache.system`, `cache.validator`, `cache.serializer`, `cache.property_info`, `cache.app`, `cache.doctrine.*`). To opt
any of them out, use `exclude_pools`. If an entry is a valid regular expression, it matches as a pattern. Otherwise, it
matches as an exact service id. Before 0.43.0, a bug skipped those pools. Read the [upgrade
note](/documentation/upgrading.md#upgrading-from-042x-to-043x) for details.

##### Interface aliases the decorator cannot satisfy

FrameworkBundle aliases `Psr\Cache\CacheItemPoolInterface`, `Symfony\Contracts\Cache\CacheInterface` and
`Symfony\Contracts\Cache\NamespacedPoolInterface` to `cache.app`. The traceable decorator implements the first two, but
not `NamespacedPoolInterface`. Thus, with the decorator on that alias, `bin/console lint:container` fails on
`CheckAliasValidityPass` (framework-bundle >= 7.3).

For this reason, the bundle points each such alias back at the undecorated pool. The alias then continues to resolve to
a class that honors its contract. The result:

| How you inject the pool                                                     | What you get |
|-----------------------------------------------------------------------------|--------------|
| by service id (`cache.app`), or `CacheItemPoolInterface` / `CacheInterface` | traced       |
| by `NamespacedPoolInterface`                                                | not traced   |

The same rule applies to PSR-18 client instrumentation and FrameworkBundle's
`Http\Client\HttpAsyncClient` alias.

##### Deferred writes on a Doctrine DBAL cache pool

A cache pool on `cache.adapter.doctrine_dbal` defers writes. It flushes them from its own commit path at process
shutdown, outside any request or message span. Each such write then appears as an orphan `doctrine.dbal.*` trace, one
per query. The trace is a `BEGIN TRANSACTION` span around `statement.prepare`/`execute`.

`flush_deferred: true` drains those deferred writes at the end of a request or command, and after each consumed message.
It does this in a single `cache.flush` span. Thus, the writes group under one trace, and the pool has nothing left to
flush at shutdown. It commits the untraced inner adapters, so idle pools do not emit empty spans for each pool. This
option commits deferred writes a little earlier: at termination, not at destruction.

To drop the cache SQL completely instead of grouping it, add the backing table to [`dbal.exclude_tables`](#doctrine-dbal), for
example `cache_items`.

The cache instrumentation also emits hit and miss counters. To export them, configure a `meter_provider`:

| Metric              | Instrument | Unit         | Emitted             |
|---------------------|------------|--------------|---------------------|
| `flow.cache.hits`   | Counter    | `operations` | once per cache hit  |
| `flow.cache.misses` | Counter    | `operations` | once per cache miss |

### Web Profiler

Adds a Flow Telemetry panel to the Symfony Web Profiler. The panel shows the signals that the bundle captured during the
current request. These are the resource attributes, spans as a timeline waterfall, the instrumentation scopes that
produced them, metrics, and logs (with `capture_logs` enabled). Thus, you can see telemetry locally without an external
OTLP backend. The toolbar shows the total signal count, and the panel shows the count for each signal type. It also
lists the configured instruments (named tracers, meters, loggers, and their scope attributes). Thus, you see each
instrument, also one that emitted nothing in this request.

```yaml
flow_telemetry:
  profiler:
    # enabled: null (default) auto-enables when WebProfilerBundle is registered; true forces it on
    # (throws if WebProfilerBundle is absent); false forces it off.
    enabled: ~
    capture_logs: false # also capture logs and render them in the panel's Logs section (off by default)
```

When the profiler is enabled, the bundle copies every exported trace and metric batch into a shared in-memory store. It
decorates the exporters that `tracer_provider` / `meter_provider` refer to (and, with `capture_logs: true`, the
exporters of `logger_provider`). OTLP export is unaffected. The real exporter still receives every batch.

The store is exposed under the stable, public service id `flow.telemetry.profiler.store`.

### Named Instruments

Configure named tracers, meters, and loggers with custom attributes.

Options:

| Option              | Type   | Default     | Description                                         |
|---------------------|--------|-------------|-----------------------------------------------------|
| `version`           | string | `'unknown'` | Instrumentation scope version                       |
| `schema_url`        | string | `null`      | Schema URL for semantic conventions                 |
| `attributes.scope`  | object | `{}`        | Attributes attached to the instrumentation scope    |
| `attributes.signal` | object | `{}`        | Default attributes merged into every emitted signal |

`attributes` has two sub-keys:

- `scope`: attributes attached to the instrumentation scope itself (shared by every signal emitted through this
  tracer/meter/logger). These are the attributes that a `sources: [scope]` filter inspects.
- `signal`: default attributes that the bundle merges into every signal (each span, metric data point, or log record) of
  this instrument. Attributes from the call site (for example, `$logger->info('...', ['env' => 'dev'])`) override the
  configured defaults with the same key. These are the attributes that a `sources: [signal]` filter inspects (the
  default).

```yaml
flow_telemetry:
  tracers:
    my_tracer:
      version: '1.0.0'  # default: 'unknown'
      schema_url: 'https://opentelemetry.io/schemas/1.21.0'
      attributes:
        scope:
          custom.attribute: 'value'
        signal:
          deployment.environment: 'prod'

  meters:
    my_meter:
      version: '1.0.0'  # default: 'unknown'
      schema_url: null
      attributes:
        signal:
          deployment.environment: 'prod'

  loggers:
    my_logger:
      version: '1.0.0'  # default: 'unknown'
```

### Main Logger

The bundle depends on the [PSR-3 Telemetry Bridge](/documentation/components/bridges/psr3-telemetry-bridge.md). It
registers a PSR-3 wrapper service for every named Telemetry logger at `flow.telemetry.<name>.logger.psr3`. Thus, you can
use Flow Telemetry loggers as the Symfony `logger` service. If telemetry is the only logging destination, you do not
need Monolog.

The bundle also always registers a `default` logger, meter, and tracer: `flow.telemetry.default.logger`,
`flow.telemetry.default.logger.psr3`, `flow.telemetry.default.meter`, `flow.telemetry.default.tracer`. It does this for
all configurations under `loggers`/`meters`/`tracers`. You can define your own `default` entry under those keys. It
overrides the automatic default.

Options:

| Option             | Type             | Default | Description                                                                                                  |
|--------------------|------------------|---------|--------------------------------------------------------------------------------------------------------------|
| `framework_logger` | `string \| null` | `null`  | Name of a logger configured under `loggers` (or the always-available `default`) to alias as Symfony `logger` |

Behavior:

- When `framework_logger` is set, the bundle aliases the Symfony `logger` service to
  `flow.telemetry.<framework_logger>.logger.psr3`. If no logger with that name exists, container compilation fails with
  a clear error.
- When `framework_logger` is `null` and Symfony's `logger` service is the default
  `Symfony\Component\HttpKernel\Log\Logger`, the bundle automatically aliases `logger` to
  `flow.telemetry.default.logger.psr3`.
- When `framework_logger` is `null` and another bundle provides `logger` (for example, Monolog or a custom alias), the
  bundle does not change `logger`.

```yaml
flow_telemetry:
  loggers:
    app:
      version: '1.0.0'

  framework_logger: app   # Symfony "logger" service -> flow.telemetry.app.logger.psr3
```

### Logging Channels

Channels send different parts of your application to different telemetry loggers, without Monolog. They are the Flow
Telemetry equivalent of Monolog channels. Each channel is a [named logger](#named-instruments). Its messages carry a
`flow.log.channel` attribute, so you can filter and group them per channel in your backend.
[`channel_attribute_target`](#channel-attribute-placement) controls where that attribute goes: the instrumentation
scope, every emitted record, or both.

A service opts into a channel with the `flow.telemetry.channel` tag. The recommended way is the
`#[WithTelemetryChannel]` attribute:

```php
<?php

namespace App\Messaging;

use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Psr\Log\LoggerInterface;

#[WithTelemetryChannel('events')]
final class OrderSubscriber
{
    public function __construct(
        private readonly LoggerInterface $logger, // resolves to the "events" channel logger
    ) {
    }
}
```

The autowired `LoggerInterface` now resolves to `flow.telemetry.events.logger.psr3`, not to the default logger. A tagged
service can also typehint the native `Flow\Telemetry\Logger\Logger`. It resolves to the native logger of that channel
(`flow.telemetry.events.logger`). The PSR-3 wrapper delegates to this instance. The bundle binds the PSR-3 interface and
the native class, so both typehints work.

The channel can also be requested with a raw tag in `services.yaml`:

```yaml
services:
  App\Messaging\OrderSubscriber:
    tags:
      - { name: 'flow.telemetry.channel', channel: 'events' }
```

Behavior:

- The bundle creates each distinct channel on demand as `flow.telemetry.<channel>.logger`, with its PSR-3 wrapper
  `flow.telemetry.<channel>.logger.psr3`. The channel has a `flow.log.channel: <channel>` attribute, placed as
  [`channel_attribute_target`](#channel-attribute-placement) sets. If a logger with that name already exists, the bundle
  uses it unchanged. Thus, the bundle adds the `flow.log.channel` attribute only to loggers that it creates.
- To send a service to the main logger of the bundle, use the `default` channel (`#[WithTelemetryChannel('default')]`).
  A `default` logger always exists, so the bundle uses it as it is and does not create it again. All channel names
  behave in the same way. This includes `app`, which has no special meaning here.
- For every channel in use, the bundle registers two named-argument autowiring aliases:
  `LoggerInterface $<channel>Logger` (the PSR-3 wrapper) and `Logger $<channel>Logger` (the native Flow `Logger`). Thus,
  any service can request a channel logger by argument name without the tag. For example, use
  `LoggerInterface $eventsLogger` or `Logger $httpClientLogger`.
- On a tagged service, the bundle also rewrites an explicit `@logger` reference to the channel logger. This applies to
  constructor arguments and to method calls (for example, `setLogger()`). The invalid-behavior flag of the reference
  stays the same.
- The bundle does not overwrite a channel that is already declared under `loggers`. Thus, you can customize its
  `version`, `schema_url`, or `attributes`. A declared channel opts out of the automatic creation. To get the
  `flow.log.channel` attribute, set it yourself:

```yaml
flow_telemetry:
  loggers:
    events:
      version: '1.0.0'
      attributes:
        scope:
          flow.log.channel: events    # not auto-added for a declared logger - set it explicitly
          team: checkout
```

#### Capturing Framework Channels

The services of Symfony already declare channels with the `monolog.logger` tag. Examples are the router, the request
logger, the event dispatcher, the HTTP client, cache pools, and the messenger. The tag comes from FrameworkBundle, with
or without MonologBundle. If you enable `capture_framework_channels`, the bundle consumes that tag. It sends these
framework channels to Flow telemetry loggers in exactly the same way as MonologBundle. Thus, it replaces the channel
routing of Monolog, without Monolog.

It is disabled by default, because MonologBundle uses the same `monolog.logger` tag. If both bundles are installed and
both process the tag, they compete for the same services. Enable it only in a setup where Flow telemetry owns the
channel routing, that is, a setup without MonologBundle:

```yaml
flow_telemetry:
  capture_framework_channels: true   # default: false
```

Each framework channel becomes `flow.telemetry.<channel>.logger` (for example, `flow.telemetry.router.logger`,
`flow.telemetry.http_client.logger`). It carries the `flow.log.channel` attribute, placed as
[`channel_attribute_target`](#channel-attribute-placement) sets. It also gets the `LoggerInterface $<channel>Logger` /
`Logger $<channel>Logger` autowiring aliases. This is the same as for an explicitly tagged service. A channel that you
declare under `loggers` still wins, so you can customize the scope of any framework channel.

The bundle always routes services that opt in explicitly through `#[WithTelemetryChannel]` / the
`flow.telemetry.channel` tag. This flag does not change that.

#### Channel Attribute Placement

The bundle can attach the generated `flow.log.channel` attribute to the instrumentation scope, to every emitted signal
(log record), or to both. `channel_attribute_target` controls this for all generated channel loggers, from framework
capture and from `#[WithTelemetryChannel]`:

```yaml
flow_telemetry:
  channel_attribute_target: both   # both (default) | scope | signal
```

- `scope`: `flow.log.channel` sits on the instrumentation scope. Filter by channel with an
  [`attribute_filtering`](#attribute_filtering) processor and `sources: [scope]`. It is not on individual records.
- `signal`: the bundle merges `flow.log.channel` into every emitted record. Thus, it is visible on each record, and you
  can filter it with the default `sources: [signal]`. This is the closest match to how Monolog stamps the channel on
  each record.
- `both` (default): on the scope and on every record. Thus, you can filter it both ways, but each record stores the
  attribute.

This applies only to channels that the bundle creates. A channel that you declare yourself under `loggers` opts out of
the automatic creation. For such a channel, set `flow.log.channel` explicitly on `attributes.scope`,
`attributes.signal`, or both.

> [!NOTE]
> `capture_framework_channels` rewrites the `logger` reference on framework services to their channel logger. A
> service-specific channel takes precedence over the global [`framework_logger`](#main-logger) redirect.

#### Coexisting with MonologBundle

If `capture_framework_channels` stays `false` (the default), you can run this bundle together with MonologBundle. The
two are independent, because they read different DI tags. Framework capture reads the Symfony `monolog.logger` tag. The
`#[WithTelemetryChannel]` attribute uses the `flow.telemetry.channel` tag of this bundle, and Monolog never reads it.
With the flag off, this bundle never touches `monolog.logger`, so:

- MonologBundle fully owns every framework channel (router, request, Doctrine, and others) and the default `app` logger.
- `#[WithTelemetryChannel('events')]` still scopes one class. It binds only the `LoggerInterface` (and the native
  `Logger`) of that service to the Flow telemetry channel logger. The binding fills the argument directly. Thus, for
  that one class, it wins over the global `LoggerInterface` autowiring alias of Monolog. Every other class continues to
  resolve to Monolog.

Do not enable `capture_framework_channels` and keep MonologBundle at the same time. Both passes then rewrite the same
services with the `monolog.logger` tag, and the result depends on the order of the compiler passes. Ownership of the
framework channels is all-or-nothing:

- Flow telemetry owns the framework channels: `capture_framework_channels: true`, MonologBundle not installed.
- Monolog owns the framework channels, and Flow scopes specific classes: `capture_framework_channels: false`,
  MonologBundle installed.

The `#[WithTelemetryChannel]` attribute works in both setups.

## Pattern Matching

Several configuration options support pattern matching for exclusion lists (paths, commands, templates, and others).

### Exact String Matching

Patterns without regex delimiters match exactly:

```yaml
exclude_paths:
  - path: '/_profiler'    # Matches exactly /_profiler
  - path: '/health'       # Matches exactly /health
```

### Regex Matching

Patterns in `/` delimiters are regular expressions:

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

### Propagating Trace Context to the Browser

When `twig/twig` is installed, the bundle registers Twig helpers that give the current trace context. Thus, you can
continue a trace into AJAX requests or multi-step flows.

| Helper                        | Returns                                                              |
|-------------------------------|----------------------------------------------------------------------|
| `flow_traceparent()`          | the W3C `traceparent` string (empty without a request span)          |
| `flow_trace_context()`        | array of propagation fields (`traceparent`, `tracestate`, `baggage`) |
| `flow_trace_context_meta()`   | one `<meta name="..." content="...">` tag per field (HTML-safe)      |
| `flow_trace_context_url(url)` | the URL with the context appended to its query string                |

AJAX (headers). Render the context as meta tags and send them back as request headers. The next request continues the
trace automatically, because `context_propagation` extracts them:

```twig
{# templates/base.html.twig #}
<head>
    {{ flow_trace_context_meta() }}
</head>
```

```js
// Forward every propagation field (traceparent, tracestate, baggage), not just traceparent.
const headers = {};
document
    .querySelectorAll('meta[name="traceparent"], meta[name="tracestate"], meta[name="baggage"]')
    .forEach((meta) => {
        headers[meta.name] = meta.content;
    });

// Only send to your own API origins so trace IDs do not leak to third parties.
fetch('/api/orders', {headers});
```

Links / multi-step flows (query string). A normal navigation cannot send headers, so carry the context in the URL
and enable query extraction:

```twig
<a href="{{ flow_trace_context_url(path('checkout_step_2')) }}">Continue</a>
```

```yaml
flow_telemetry:
  instrumentation:
    http_kernel:
      context_propagation: true
      context_propagation_query: true
```

From a controller or service (PHP). The same helpers are available without Twig. For the raw context, inject
`TraceContextProvider`. You can also inject `TraceContextUrlGenerator`, an opt-in wrapper around the router. It does not
replace the router, so `path()`/`url()` stay unchanged:

```php
use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;

final class CheckoutController extends AbstractController
{
    public function __construct(
        private readonly TraceContextProvider $traceContext,
        private readonly TraceContextUrlGenerator $traceContextUrls,
    ) {
    }

    public function step1(): Response
    {
        // append to a URL you generated yourself...
        $next = $this->traceContext->appendToUrl($this->generateUrl('checkout_step_2'));

        // ...or let the wrapper generate + append in one call
        $next = $this->traceContextUrls->generate('checkout_step_2');

        // raw fields, e.g. to forward as headers on an outgoing call
        $headers = $this->traceContext->current(); // ['traceparent' => ..., 'baggage' => ...]

        return $this->redirect($next);
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
          timeout_ms: 250
          connect_timeout_ms: 250
          headers:
            Authorization: 'Bearer %env(OTEL_AUTH_TOKEN)%'
          encoding: protobuf
          failover:
            type: stream
            endpoint: '%kernel.logs_dir%/otel-traces-failed.jsonl'
    otlp_metrics:
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)%'
          encoding: protobuf
    otlp_logs:
      otlp:
        transport:
          type: curl
          endpoint: '%env(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT)%'
          encoding: protobuf

  tracer_provider:
    sampler:
      type: parent_based
      root:
        type: trace_id_ratio
        ratio: 0.1  # sample 10% of traces that originate here; upstream decisions are always honoured
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
      type: pipeline
      middleware:
        - { type: severity_filtering, minimum_severity: info }
      sink:
        type: batching
        exporter: otlp_logs

  loggers:
    app:
      version: '%env(APP_VERSION)%'
    audit:
      version: '%env(APP_VERSION)%'
      attributes:
        scope:
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
      trace: true
    dbal:
      enabled: true
      max_sql_length: 500
    cache:
      enabled: true
      exclude_pools:
        - 'cache.system'
```
