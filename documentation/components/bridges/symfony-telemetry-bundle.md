---
package: flow-php/symfony-telemetry-bundle
---

# Symfony Telemetry Bundle

Flow Symfony Telemetry Bundle provides automatic telemetry integration for Symfony applications, including HTTP
request/response tracing, console command instrumentation, and configurable exporters through OpenTelemetry-compatible
backends.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/symfony-telemetry-bundle.md).

## Overview

This bundle is built on top of [flow-php/telemetry](/documentation/components/libs/telemetry.md) — see that page for the
underlying `Telemetry`, tracer/meter/logger API, and processor/exporter primitives. For exporting to OTLP-compatible
backends, it also uses the [Telemetry OTLP Bridge](/documentation/components/bridges/telemetry-otlp-bridge.md).

This bundle integrates Flow PHP's Telemetry library with Symfony applications. It provides:

- **Automatic resource detection** - Detects service name, version, environment, OS, host, and process information
- **8 auto-instrumentation options** - HTTP kernel, console, messenger, Twig, HTTP client, PSR-18 client, Doctrine DBAL,
  and cache
- **Context propagation** - W3C TraceContext and Baggage support for distributed tracing
- **Configurable exporters** - Console, memory, void, or OTLP with multiple transport options
- **Full Symfony configuration** - Configure everything through Symfony's config system

## Configuration Reference

> Any `error_handler:` field that appears under a provider, processor, or `otlp` exporter references a name from the
> top-level `error_handlers:` map (see [Error Handlers](#error-handlers)). When omitted it defaults to `default`, which
> is auto-created as `{ type: error_log }` if the user does not declare one.

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

### Error Handlers

Per the OpenTelemetry spec, the SDK MUST NOT throw to user code at runtime. Errors raised by exporters/processors are
caught and forwarded to a configured error handler. The bundle exposes a **named map** under `error_handlers:` that is
referenced from provider, processor, and OTLP exporter blocks via `error_handler:` fields.

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

If `error_handlers:` is omitted (or `default` is missing inside it), the bundle injects
`error_handlers.default = { type: error_log }` automatically so every `error_handler:` reference always resolves.

Service IDs registered by the bundle (predictable for `decorates:`):

- `flow.telemetry.error_handler.<name>` — e.g. `flow.telemetry.error_handler.default`

#### error_log (default)

Writes formatted Throwables via PHP's `error_log()` — stderr in CLI by default, or the `error_log` ini setting
otherwise.
Matches the OTEL spec recommendation to log to standard error output.

| Option            | Type    | Default            | Description                                                   |
|-------------------|---------|--------------------|---------------------------------------------------------------|
| `message_type`    | enum    | `operating_system` | `operating_system` (0), `email` (1), `file` (3), `sapi` (4)   |
| `expand_newlines` | boolean | `false`            | Emit one `error_log()` call per line of the formatted message |
| `message_prefix`  | string  | `[flow-telemetry]` | Prefix prepended to every message                             |

#### stream

Appends formatted Throwables (one per line) to a file path or `php://` stream wrapper. The handle is opened lazily on
the first call and reused.

| Option               | Type    | Default            | Description                                                |
|----------------------|---------|--------------------|------------------------------------------------------------|
| `destination`        | string  | -                  | File path or `php://stdout`/`php://stderr`/etc. (required) |
| `file_permissions`   | integer | `0644`             | Permissions for newly created files (ignored for `php://`) |
| `create_directories` | boolean | `true`             | Create parent directories of the destination if missing    |
| `message_prefix`     | string  | `[flow-telemetry]` | Prefix prepended to every line                             |

#### syslog

Writes via `openlog/syslog/closelog`.

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

Fans an error out to multiple named handlers. Each child invocation is wrapped so a misbehaving handler cannot prevent
siblings from running.

| Option     | Type            | Default | Description                                            |
|------------|-----------------|---------|--------------------------------------------------------|
| `handlers` | list of strings | -       | Names of other entries in `error_handlers:` (required) |

```yaml
fanout:
  type: composite
  handlers: [ default, to_file ]
```

#### noop

Discards every Throwable. Intended for tests or explicit silence.

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

The bundle exposes a **top-level named map** of exporters. The implementation is selected by the **sub-block name**
under each entry — there is no separate `type:` key. Each exporter must declare exactly one of the supported
sub-blocks: `otlp`, `service`, `console`, `memory`, `void`. Per-signal processors reference exporters by name.

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

Every exporter also accepts an `enabled` flag (default `true`). When it resolves to `false`, the exporter does not
ship anything to its backend, but **profiler capture and all other exporters keep working** — so telemetry stays
visible in the Symfony profiler without being sent to a collector.

The flag accepts a literal boolean or an environment variable:

- A literal `enabled: false` is resolved at compile time and replaces the exporter with a no-op (`void`); the real
  backend (e.g. the OTLP transport) is never built.
- An `enabled: '%env(bool:OTEL_ENABLED)%'` is resolved per request at runtime: the real exporter is wrapped so the
  env var gates export on or off without rebuilding the container. The backend is still built, so a declared `otlp`
  exporter requires `flow-php/telemetry-otlp-bridge` to be installed even while the flag is off.

The exporter still declares its real sub-block in both cases, so toggling it back on needs no other change.

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

- `flow.telemetry.exporter.<name>` — e.g. `flow.telemetry.exporter.otlp`
- `flow.telemetry.exporter.<name>.transport` — only when the exporter uses the `otlp` sub-block

### TracerProvider

Configures the tracer provider for distributed tracing.

```yaml
flow_telemetry:
  tracer_provider:
    error_handler: default  # name from error_handlers; defaults to "default"
    sampler:
      type: always_on   # always_on|always_off|trace_id_ratio|parent_based|attribute_matching|service
      ratio: 1.0        # Sampling ratio (0.0-1.0, only for trace_id_ratio)
      service_id: null  # Custom sampler service (only for type: service)
    processor:
      type: batching    # composite|memory|batching|passthrough|void|attribute_filtering|service
      batch_size: 512
      exporter: otlp    # name of a top-level exporter
      error_handler: default
```

**Sampler types:**

| Type                 | Description                                                                                       |
|----------------------|---------------------------------------------------------------------------------------------------|
| `always_on`          | Sample all traces (default)                                                                       |
| `always_off`         | Sample no traces                                                                                  |
| `trace_id_ratio`     | Sample based on trace ID ratio                                                                    |
| `parent_based`       | Respect parent span's sampling decision                                                           |
| `attribute_matching` | Drop spans matching an attribute matcher (start-time attrs); defer the rest to a delegate sampler |
| `service`            | Custom sampler service                                                                            |

**`attribute_matching`** is the OTel-idiomatic way to drop spans by attribute — the decision is made at span start, so a
matched span never records or reaches a processor. It only sees attributes available at span start (not end-state values
like status codes). It reuses the same `matcher` / `exclude` / `sources` / `cache_dir` options as the
[`attribute_filtering`](#attribute_filtering) processor, plus a `delegate` sampler for spans that don't match:

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

**Pipeline** (logs only) — chain middleware (enrich/filter) before a terminal sink:

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

The Flow equivalent of Symfony's Monolog `ConsoleHandler`: while a console command runs,
emitted log records are also printed to that command's output, filtered by the command's
verbosity (`-v`/`-vv`/`-vvv`). This is **display only** — it does not change what the
configured processor exports (OTLP, etc.). It is **off by default**; like MonologBundle's
console handler, enable it explicitly (typically only in `dev`):

```yaml
# config/packages/flow_telemetry.yaml
when@dev:
  flow_telemetry:
    logger_provider:
      console_output:
        enabled: true
```

The verbosity → minimum-severity thresholds default to:

| Verbosity                | Flag   | Minimum severity |
|--------------------------|--------|------------------|
| `VERBOSITY_QUIET`        | `-q`   | `ERROR`          |
| `VERBOSITY_NORMAL`       | (none) | `ERROR`          |
| `VERBOSITY_VERBOSE`      | `-v`   | `WARN`           |
| `VERBOSITY_VERY_VERBOSE` | `-vv`  | `INFO`           |
| `VERBOSITY_DEBUG`        | `-vvv` | `DEBUG`          |

Flow has no `NOTICE` level (the PSR-3 bridge collapses `NOTICE` into `INFO`), so there is
no rung between `WARN` and `INFO`. The defaults keep the terminal quiet by default (errors
only) and reveal exactly one more severity per `-v` step.

`-vvv` is the most verbose flag Symfony has and the default map stops at `DEBUG`, so with
the defaults `TRACE` logs are never echoed to the console (they still reach your exporters,
e.g. OTLP). To see `TRACE` on the console, remap a flag to it with `verbosity_levels` —
keyed by the Symfony verbosity constant name, valued by a Flow severity name
(`TRACE|DEBUG|INFO|WARN|ERROR|FATAL`). For example, point `-vvv` at `TRACE`:

```yaml
flow_telemetry:
  logger_provider:
    console_output:
      enabled: true
      verbosity_levels:
        VERBOSITY_DEBUG: TRACE   # -vvv now shows TRACE and everything above it
```

You can override any rung the same way — for instance, show `INFO` and above with no flag at all:

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

| Option          | Type    | Default | Description                                                                                                                     |
|-----------------|---------|---------|---------------------------------------------------------------------------------------------------------------------------------|
| `batch_size`    | integer | `512`   | Number of items per batch                                                                                                       |
| `max_batch_age` | float   | `null`  | Max seconds before a partial batch is force-exported, measured from the first buffered signal; `null` disables time-based flush |
| `exporter`      | string  | -       | Name of a top-level exporter (required)                                                                                         |

```yaml
processor:
  type: batching
  batch_size: 512
  max_batch_age: 15.0   # export at least every 15s in long-running processes
  exporter: otlp
```

By default a batch is exported only when it reaches `batch_size`, when `flush()`/`shutdown()` is called, or
when the process exits. In a request-scoped app that is fine — the request ends and the buffer drains. In a
**long-running process** (Symfony Messenger workers, daemons, or persistent runtimes such as ReactPHP,
RoadRunner, Swoole, AMP, or FrankenPHP worker mode) a low-rate signal can sit in the buffer until 512
accumulate or the process stops. Set `max_batch_age` (e.g. `15.0`) to bound how long any single signal waits
before export; leave it unset for request-scoped apps where behavior is unchanged.

> **PHP limitation — idle processes.** PHP has no background timer thread, so the age deadline is evaluated
> only when a signal is processed, not on a wall-clock schedule. A **fully idle** process — nothing ending
> spans or processing metrics/logs — cannot self-flush a partial batch on the age trigger. That residual
> case needs an external flush: the Messenger worker-event flush subscriber, a runtime loop calling
> `Telemetry::flush()`, or `shutdown()` when the process exits. The deadline is measured from a monotonic
> clock (`hrtime`), so it is immune to wall-clock adjustments.

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

Runs each log record through an ordered list of **middleware** (enrich / filter), then forwards the survivors to a
single **sink**. This is the log-only way to combine more than one processing step (the OpenTelemetry
`LogRecordProcessor` model: each step may modify the record or drop it, with changes visible to the next).

| Option       | Type   | Default      | Description                                                                                                        |
|--------------|--------|--------------|--------------------------------------------------------------------------------------------------------------------|
| `middleware` | list   | `[]`         | Ordered middleware; the first to drop a record short-circuits the rest                                             |
| `sink`       | object | – (required) | Terminal processor (`memory`, `batching`, `passthrough`, `void`, `service`, or `composite`) that exports survivors |

Each `middleware` entry has a `type` and its own options:

| `type`                | Options                                                                                        | Effect                                           |
|-----------------------|------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `enriching`           | `attributes` (map)                                                                             | Merges default attributes (call-site values win) |
| `severity_filtering`  | `minimum_severity` (`trace`…`fatal`, default `info`)                                           | Drops records below the level                    |
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

Drops (or keeps) signals based on their attributes — useful for reducing noise, e.g. discarding health-check or bot
traffic before it is exported. On `tracer_provider` and `meter_provider` it is a **processor** that wraps an
`inner_processor` (which receives the survivors). On `logger_provider` it is instead a **middleware** inside a
[`pipeline`](#pipeline-logger_provider-only) (no `inner_processor` — the pipeline's `sink` receives the survivors). The
`matcher`/`exclude`/`sources`/`cache_dir` options below apply in both forms.

| Option                  | Type         | Default                                     | Description                                                                                                                                                                                                                                             |
|-------------------------|--------------|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `matcher`               | object       | – (required)                                | The matcher tree (see below)                                                                                                                                                                                                                            |
| `exclude`               | boolean      | `true`                                      | `true`: drop matching signals; `false`: keep ONLY matching signals                                                                                                                                                                                      |
| `sources`               | list of enum | `[signal]`                                  | Which attribute sets to inspect: any of `signal`, `resource`, `scope`. The matcher is evaluated against each listed source and OR-combined — a signal matches if it matches in **any** source                                                           |
| `cache_dir`             | string       | `%kernel.cache_dir%/flow_telemetry_filters` | Directory for the compiled matcher cache                                                                                                                                                                                                                |
| `cache_dir_permissions` | int          | `0o700`                                     | Octal mode applied when the cache directory is **created** (owner-only by default, since it holds `require`d PHP; subject to umask). Write it as a YAML octal literal, e.g. `0o750`. Only applies on creation — an existing directory is left untouched |
| `inner_processor`       | object       | – (required for span/metric)                | **tracer/meter only** — the wrapped processor that receives surviving signals (leaf types only: `memory`, `batching`, `passthrough`, `void`, `service`). For logs, omit it: the [`pipeline`](#pipeline-logger_provider-only) `sink` receives survivors  |

The `matcher` is a tree. Each node is **exactly one** of:

- `all: [ … ]` — a list of child matchers; matches when **every** child matches (AND).
- `any: [ … ]` — a list of child matchers; matches when **at least one** child matches (OR).
- `not: { … }` — a single child matcher; matches when the child does **not**.
- a **leaf rule** carrying a `path` (the four kinds are mutually exclusive — mixing them in one node is a configuration
  error).

Composites and leaves nest to any depth, so arbitrary combinations of ANDs, ORs and NOTs are expressible. A leaf rule
accepts:

| Option           | Type         | Default  | Description                                                              |
|------------------|--------------|----------|--------------------------------------------------------------------------|
| `path`           | string\|list | required | Attribute key, or a list of segments descending into nested array values |
| `mode`           | enum         | required | Comparison mode (see below)                                              |
| `value`          | scalar       | required | Expected value (must be a string for the pattern modes)                  |
| `case_sensitive` | boolean      | `true`   | Substring modes only                                                     |

**Modes:** `equal`, `not_equal`, `greater_than`, `greater_than_equal`, `less_than`, `less_than_equal`, `regexp`,
`starts_with`, `ends_with`, `contains`.

Drop health-check and bot logs (drop if **any** branch matches) — on `logger_provider`, attribute filtering is a
middleware inside a [`pipeline`](#pipeline-logger_provider-only); the `sink` receives the survivors:

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

Keep **only** payments-scope metrics that are not internal (`exclude: false`, `sources: [scope]`, negation):

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

Inspect more than one source at once — drop a signal whose route is `/health` whether that attribute sits on the
signal itself **or** on the resource:

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

Deeply nested combinations — drop a span when it is a fast internal call **or** a non-health route on a flagged tenant
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

**Filtering logs by severity (logs only).** Severity is not an attribute, but on `logger_provider` the
`attribute_filtering` middleware exposes the record's severity to the matcher as two reserved **signal** keys, so
per-channel (or per-attribute) severity thresholds are expressible alongside any other attribute:

- `log.severity` — the OpenTelemetry severity **number** (`trace`=1, `debug`=5, `info`=9, `warn`=13, `error`=17,
  `fatal`=21), for ordered comparisons (`greater_than_equal`, …).
- `log.severity_name` — the level **name** (`TRACE`…`FATAL`), for `equal`/`regexp`.

These keys exist only for the filter decision — they are never exported (severity already travels in the native OTLP
`severityNumber`/`severityText` fields) and they shadow any user attribute of the same name. Because they live on the
`signal` source, an `all` node that combines severity with another attribute needs that attribute on `signal` too — the
channel's `log.channel` attribute is on `signal` under the default `channel_attribute_target: both`.

Keep `error`+ from the `payments` channel but `debug`+ from the `importer` channel (`exclude: false` keeps only
matching records — exactly the case a single global [`severity_filtering`](#pipeline-logger_provider-only) threshold
cannot express):

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
                  - { path: log.channel, mode: equal, value: payments }
                  - { path: log.severity, mode: greater_than_equal, value: 17 }   # error+
              - all:
                  - { path: log.channel, mode: equal, value: importer }
                  - { path: log.severity, mode: greater_than_equal, value: 5 }    # debug+
      sink:
        type: batching
        exporter: otlp
```

For a single global threshold prefer the simpler `severity_filtering` middleware; reach for `log.severity` only when
the threshold varies by channel or another attribute.

The matcher is compiled to a cached PHP matcher in `cache_dir` (the project cache directory by default) so per-signal
evaluation stays cheap; it falls back to interpreted matching when the directory is not writable. Because the compiled
file is `require`d, `cache_dir` must be **trusted** (not writable by untrusted users) — the project cache directory is
application-private, so the default is safe.

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
      error_handler: default   # name from error_handlers; defaults to "default"
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        encoding: protobuf
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

#### Transport type

| `type`       | Transport            | Notes                                                                                                                                                     |
|--------------|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `curl`       | `CurlTransport`      | Synchronous HTTP (default). Each `send()` blocks until the response.                                                                                      |
| `async_curl` | `AsyncCurlTransport` | Non-blocking `curl_multi`. Auto-pumped on Messenger's `WorkerRunningEvent` when `messenger` instrumentation is enabled; otherwise pump `tick()` yourself. |
| `grpc`       | `GrpcTransport`      | OTLP/gRPC (Protobuf only).                                                                                                                                |
| `stream`     | `StreamTransport`    | JSONL to a file path or `php://` stream.                                                                                                                  |
| `service`    | user service         | Aliases an existing transport service id.                                                                                                                 |

Prefer `curl`. Use `async_curl` only for non-blocking dispatch — in a Messenger worker the bundle pumps it on
`WorkerRunningEvent`; elsewhere you must call `tick()` yourself. See the
[OTLP bridge async transport section](/documentation/components/bridges/telemetry-otlp-bridge.md#async-curl-transport).

#### Timeouts

The defaults assume the recommended deployment: an OpenTelemetry Collector running close to the application (loopback,
UDS, or sidecar):

| Setting               |                                 Default | Applies to             | Bounds                                                      |
|-----------------------|----------------------------------------:|------------------------|-------------------------------------------------------------|
| `timeout_ms`          | 10000 curl / 5000 async_curl / 250 grpc | curl, async_curl, grpc | Per-request deadline (curl: total request; grpc: per-call)  |
| `connect_timeout_ms`  |              250 curl / 1500 async_curl | curl, async_curl       | TCP/TLS connect; gRPC has no separate bound                 |
| `pump_timeout_ms`     |                                     100 | async_curl only        | Per-`tick()` bounded drive budget (`0` = single exec round) |
| `shutdown_timeout_ms` |                                    5000 | curl, async_curl, grpc | Wall-clock budget for draining pending requests at shutdown |

`curl` is synchronous, so its `timeout_ms` is a per-flush ceiling. `async_curl` uses a larger **1500 ms**
`connect_timeout_ms` (it only advances when pumped) and a **100 ms** `pump_timeout_ms` per `tick()`. gRPC progresses in
the background, so its per-call deadline stays at **250 ms**. `shutdown_timeout_ms` bounds graceful drain at exit. See
the [OTLP bridge Timeouts section](/documentation/components/bridges/telemetry-otlp-bridge.md#timeouts).

#### Failover Transport

Both `curl` and `grpc` transports accept an optional nested `failover:` block. When primary delivery fails, the prior
batch is forwarded to the failover transport and a `FailoverTransportException` is raised so the operator is informed
even when failover absorbs the data. Common pattern: **gRPC primary → stream (JSONL on disk) failover** so a downed
collector still leaves recoverable data the operator can replay later.

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

**Constraints**

- The `failover:` block accepts the same fields as the parent transport, except it cannot itself declare a nested
  `failover:` (single-level depth).
- Allowed only on `curl`, `async_curl` and `grpc` primaries. `failover` under a `stream` or `service` primary is
  rejected at config-validation time.
- The bundle registers `flow.telemetry.exporter.<name>.failover.transport` for the failover service id.

For the underlying behavior — when a forwarded batch is treated as absorbed vs. lost, the shape of
`FailoverTransportException`, and the cascade-shutdown contract — see the
[OTLP bridge Failover Transport section](/documentation/components/bridges/telemetry-otlp-bridge.md#failover-transport).

#### curl (default)

| Option                | Type    | Default | Description                                                                     |
|-----------------------|---------|---------|---------------------------------------------------------------------------------|
| `endpoint`            | string  | -       | OTLP base URL (required)                                                        |
| `timeout_ms`          | integer | `5000`  | Total per-request deadline in **milliseconds**                                  |
| `connect_timeout_ms`  | integer | `250`   | TCP/TLS connect deadline in **milliseconds**                                    |
| `shutdown_timeout_ms` | integer | `5000`  | Wall-clock budget in **milliseconds** for draining pending requests at shutdown |
| `compression`         | boolean | `false` | Enable compression                                                              |
| `follow_redirects`    | boolean | `true`  | Follow HTTP redirects                                                           |
| `max_redirects`       | integer | `3`     | Maximum redirects to follow                                                     |
| `proxy`               | string  | `null`  | Proxy URL                                                                       |
| `ssl_verify_peer`     | boolean | `true`  | Verify SSL peer                                                                 |
| `ssl_verify_host`     | boolean | `true`  | Verify SSL host                                                                 |
| `ssl_cert_path`       | string  | `null`  | SSL certificate path                                                            |
| `ssl_key_path`        | string  | `null`  | SSL key path                                                                    |
| `ca_info_path`        | string  | `null`  | CA info path                                                                    |
| `headers`             | object  | `{}`    | Additional HTTP headers                                                         |
| `encoding`            | enum    | `json`  | OTLP/HTTP wire encoding: `json` or `protobuf`                                   |
| `failover`            | object  | `null`  | Optional [failover transport](#failover-transport)                              |

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

gRPC transport. `encoding` is rejected by validation (OTLP/gRPC mandates Protobuf, built internally), and
`connect_timeout_ms` is rejected because gRPC has no separate connect bound — `timeout_ms` is the per-call deadline
covering DNS, connect, send and receive together.

| Option                | Type    | Default | Description                                                                  |
|-----------------------|---------|---------|------------------------------------------------------------------------------|
| `endpoint`            | string  | -       | gRPC endpoint (required)                                                     |
| `timeout_ms`          | integer | `250`   | Per-call deadline in **milliseconds**                                        |
| `shutdown_timeout_ms` | integer | `5000`  | Wall-clock budget in **milliseconds** for draining pending calls at shutdown |
| `insecure`            | boolean | `true`  | Allow insecure connections                                                   |
| `headers`             | object  | `{}`    | gRPC metadata                                                                |
| `failover`            | object  | `null`  | Optional [failover transport](#failover-transport)                           |

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

OTLP File Exporter ([spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/)). Writes one JSON Line
per batch to the configured destination — either an absolute file path or a `php://` stream wrapper — with
`LOCK_EX` around each `fwrite`. Only JSON encoding is supported per the spec; `encoding` and HTTP-specific options
(`timeout`, `ssl_*`, `headers`, etc.) are rejected at config time.

| Option               | Type    | Default | Description                                                                               |
|----------------------|---------|---------|-------------------------------------------------------------------------------------------|
| `endpoint`           | string  | -       | File path or `php://` stream wrapper URI (required)                                       |
| `file_permissions`   | integer | `0644`  | File mode applied when creating new files; ignored for `php://` destinations              |
| `create_directories` | boolean | `true`  | Create the destination's parent directories if missing; ignored for `php://` destinations |

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

To write all three signal types to one file, point three exporters at the same destination — the OpenTelemetry
Collector's `otlpjsonfile` receiver handles mixed `resourceLogs` / `resourceMetrics` / `resourceSpans` lines.

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
        encoding: protobuf
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
          encoding: protobuf
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

In addition to the request (SERVER) span, the bundle can trace the controller lifecycle as child spans of
the request span (same instrumentation scope, kind `INTERNAL`). They are emitted only while the request span
exists, so disabling `http_kernel` or excluding the path produces none.

- `trace_controller` (default **true**) — the controller **body** execution. The span is named after the
  resolved controller (e.g. `App\Controller\OrderController::import`) and carries `code.namespace`,
  `code.function` and `controller` attributes. It starts after argument resolution and completes at
  `kernel.view`/`kernel.response`, so it excludes resolution time and appears in both the OTLP export and the
  Flow Telemetry profiler panel.
- `trace_controller_resolution` (default **false**) — controller resolution
  (`ControllerResolverInterface::getController()`), emitted as a `controller.get_callable` span.
- `trace_controller_arguments` (default **false**) — argument resolution as a single aggregate
  `controller.get_arguments` span.
- `trace_controller_argument_resolvers` (default **false**) — one `controller.argument_value_resolver` span
  per value resolver invocation (finer-grained, higher cardinality).

The resolution and argument toggles install service decorators only when enabled, so they add zero overhead
when off.

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
      propagation_style: link     # How the consumer span relates to the producer span
      link_to_worker: true        # Link each message trace back to the messenger:consume worker span
```

When `context_propagation` is enabled, `propagation_style` controls how a consumed message's span
relates to the producing (publishing) span:

- `link` (default) — each consumed message is the **root of its own trace** and carries a span link back
  to the producer span (per the OpenTelemetry messaging conventions). Producer and consumer get separate,
  clean traces connected by a link, and a long-running worker no longer collapses every message into one
  trace. Recommended for decoupled, batch, or long-delay queues, where continuing the trace would otherwise
  absorb the entire queue wait into a single span's duration.
- `continue` — the consumer span adopts the producer's trace and becomes its child, so
  publish → queue → consume is one continuous distributed trace. Fine for fast, 1:1 processing.

`propagation_style` has no effect when `context_propagation` is `false` (there is nothing to relate to).
The producer side is identical in both modes — the telemetry stamp is always written on dispatch.

`link_to_worker` (default `true`) adds a span link from each consumed message's trace back to the active
`messenger:consume` worker span, so you can pivot from a message trace to the worker that processed it.
Set it to `false` to omit the link (e.g. if the consume command is not traced).

Buffered telemetry is flushed **after each handled or failed message** while the worker keeps running, so
consumer-side traces/logs/metrics export promptly instead of only when the `messenger:consume` worker
stops. (Without it, signals emitted inside handlers sit in the batching processors — default batch size
512 — and stay invisible until the buffer fills or the worker exits.) Failures flush too, so error spans
and exception logs are visible even when the message is retried or sent to the failure transport. This is
independent of `context_propagation` / `propagation_style`.

Flushing per message means one exporter round-trip per message. For high-throughput workers, set
[`max_batch_age`](#batching) on the batching processor so the batch coalesces across messages and a single
idle worker still exports on a time bound rather than per message.

The OTLP `curl` transport sends synchronously, so the per-message flush exports the batch and reports its outcome
before the handler returns — there is nothing to pump between messages and no request left in flight to stall until
shutdown. Each flush blocks up to the curl `timeout_ms`, so keep a Collector close to the worker (loopback/UDS/sidecar)
to keep that sub-millisecond (see [Timeouts](#timeouts)).

With the `async_curl` transport, the subscriber also pumps each transport's `tick()` on every `WorkerRunningEvent`, so
in-flight requests complete in the background.

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

### Web Profiler

Adds a **Flow Telemetry** panel to the Symfony Web Profiler showing the signals captured during the
current request — spans as a timeline waterfall, metrics, and (when `capture_logs` is enabled) logs —
so telemetry is visible locally without an external OTLP backend. The toolbar shows the total signal
count; the panel breaks it down per signal type.

```yaml
flow_telemetry:
  profiler:
    # enabled: null (default) auto-enables when WebProfilerBundle is registered; true forces it on
    # (throws if WebProfilerBundle is absent); false forces it off.
    enabled: ~
    capture_logs: false # also capture logs and render them in the panel's Logs section (off by default)
```

When enabled, the bundle mirrors every exported traces/metrics batch into a shared in-memory store by
decorating the exporters the `tracer_provider` / `meter_provider` (and, with `capture_logs: true`, the
`logger_provider`) reference. OTLP export is unaffected — the real exporter still receives every batch.

The store is exposed under the stable, public service id **`flow.telemetry.profiler.store`**.

### Named Instruments

Configure named tracers, meters, and loggers with custom attributes.

**Options:**

| Option              | Type   | Default     | Description                                         |
|---------------------|--------|-------------|-----------------------------------------------------|
| `version`           | string | `'unknown'` | Instrumentation scope version                       |
| `schema_url`        | string | `null`      | Schema URL for semantic conventions                 |
| `attributes.scope`  | object | `{}`        | Attributes attached to the instrumentation scope    |
| `attributes.signal` | object | `{}`        | Default attributes merged into every emitted signal |

`attributes` has two sub-keys:

- **`scope`** — attributes attached to the instrumentation scope itself (shared by every signal emitted through this
  tracer/meter/logger). These are what a `sources: [scope]` filter inspects.
- **`signal`** — default attributes merged into every individual signal (each span/metric data point/log record)
  emitted through this instrument. Attributes passed at the call site (e.g. `$logger->info('...', ['env' => 'dev'])`)
  override the configured defaults of the same key. These are what a `sources: [signal]` filter inspects (the default).

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

The bundle depends on [PSR-3 Telemetry Bridge](/documentation/components/bridges/psr3-telemetry-bridge.md) and registers
a PSR-3 wrapper service for every named Telemetry logger at `flow.telemetry.<name>.logger.psr3`. This makes Flow
Telemetry loggers usable as Symfony's `logger` service, removing the need for Monolog when telemetry is the only logging
destination.

In addition, the bundle always registers a `default` logger, meter, and tracer — `flow.telemetry.default.logger`,
`flow.telemetry.default.logger.psr3`, `flow.telemetry.default.meter`, `flow.telemetry.default.tracer` — regardless of
what is configured under `loggers`/`meters`/`tracers`. Defining your own `default` entry under those keys is allowed and
will override the auto-default.

**Options:**

| Option             | Type             | Default | Description                                                                                                  |
|--------------------|------------------|---------|--------------------------------------------------------------------------------------------------------------|
| `framework_logger` | `string \| null` | `null`  | Name of a logger configured under `loggers` (or the always-available `default`) to alias as Symfony `logger` |

**Behavior:**

- When `framework_logger` is set, the bundle aliases the Symfony `logger` service to
  `flow.telemetry.<framework_logger>.logger.psr3`. If no logger with that name exists, container compilation fails with
  a clear error.
- When `framework_logger` is `null` and Symfony's `logger` service is the default
  `Symfony\Component\HttpKernel\Log\Logger`, the bundle automatically aliases `logger` to
  `flow.telemetry.default.logger.psr3`.
- When `framework_logger` is `null` and `logger` is provided by another bundle (Monolog, custom alias, etc.), the bundle
  leaves `logger` alone.

```yaml
flow_telemetry:
  loggers:
    app:
      version: '1.0.0'

  framework_logger: app   # Symfony "logger" service -> flow.telemetry.app.logger.psr3
```

### Logging Channels

Channels let you route different parts of your application to different telemetry loggers — the Flow Telemetry
equivalent of Monolog channels — without installing Monolog. Each channel is a [named logger](#named-instruments);
messages emitted through it carry a `log.channel` attribute, so you can filter and group them per channel in your
backend. Where that attribute is placed — the instrumentation scope, every emitted record, or both — is controlled by
[`channel_attribute_target`](#channel-attribute-placement).

A service opts into a channel by carrying the `flow.telemetry.channel` tag. The recommended way is the
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

The autowired `LoggerInterface` now resolves to `flow.telemetry.events.logger.psr3` instead of the default logger. A
tagged service may instead typehint the native `Flow\Telemetry\Logger\Logger` — it resolves to that channel's native
logger (`flow.telemetry.events.logger`), the instance the PSR-3 wrapper delegates to. Both the PSR-3 interface and the
native class are bound, so either typehint works.

The channel can also be requested with a raw tag in `services.yaml`:

```yaml
services:
  App\Messaging\OrderSubscriber:
    tags:
      - { name: 'flow.telemetry.channel', channel: 'events' }
```

**Behavior:**

- Each distinct channel is synthesized on demand as `flow.telemetry.<channel>.logger` (+ its PSR-3 wrapper
  `flow.telemetry.<channel>.logger.psr3`), carrying a `log.channel: <channel>` attribute placed per
  [`channel_attribute_target`](#channel-attribute-placement) — unless a logger of that name already exists, which is
  then
  reused untouched (so the `log.channel` attribute is only added to loggers the bundle creates).
- To route a service to the bundle's main logger, use the `default` channel (`#[WithTelemetryChannel('default')]`).
  A `default` logger always exists, so it is reused as-is rather than re-created. Every channel name — including `app`,
  which carries no special meaning here — behaves the same way.
- For every channel in use, two named-argument autowiring aliases are registered — `LoggerInterface $<channel>Logger`
  (the PSR-3 wrapper) and `Logger $<channel>Logger` (the native Flow `Logger`) — so any service can request a channel
  logger by argument name (e.g. `LoggerInterface $eventsLogger`, `Logger $httpClientLogger`) without the tag.
- On a tagged service, an explicit `@logger` reference is rewritten to the channel logger as well — in both constructor
  arguments and method calls (e.g. `setLogger()`), preserving the reference's invalid-behavior flag.
- A channel already declared under `loggers` is **not** overwritten, so you can customise its `version`, `schema_url`,
  or `attributes`. Because declaring it opts out of synthesis, set `log.channel` yourself if you want it:

```yaml
flow_telemetry:
  loggers:
    events:
      version: '1.0.0'
      attributes:
        scope:
          log.channel: events    # not auto-added for a declared logger — set it explicitly
          team: checkout
```

#### Capturing Framework Channels

Symfony's own services already declare channels by tagging themselves `monolog.logger` (the router, the request logger,
the event dispatcher, the HTTP client, cache pools, the messenger, …) — that tag comes from FrameworkBundle and is
present whether or not MonologBundle is installed. Enable `capture_framework_channels` and the bundle **consumes that
tag**, routing these framework channels to Flow telemetry loggers exactly the way MonologBundle would — making it a
drop-in replacement for Monolog's channel routing, without Monolog.

It is **disabled by default**, because MonologBundle claims the same `monolog.logger` tag: if both are installed and
both process it, they fight over the same services. Enable it only when Flow telemetry owns channel routing (i.e. you
are not running MonologBundle):

```yaml
flow_telemetry:
  capture_framework_channels: true   # default: false
```

Each framework channel becomes `flow.telemetry.<channel>.logger` (e.g. `flow.telemetry.router.logger`,
`flow.telemetry.http_client.logger`), carries the `log.channel` attribute (placed per
[`channel_attribute_target`](#channel-attribute-placement)), and gets the
`LoggerInterface $<channel>Logger` / `Logger $<channel>Logger` autowiring aliases — the same treatment as an explicitly
tagged service. A channel you declare under `loggers` still wins, so you can customise any framework channel's scope.

Services that opt in explicitly via `#[WithTelemetryChannel]` / the `flow.telemetry.channel` tag are always routed
regardless of this flag.

#### Channel Attribute Placement

The synthesized `log.channel` attribute can be attached to the instrumentation **scope**, to every emitted **signal**
(log record), or to **both**. `channel_attribute_target` controls this for **all** synthesized channel loggers —
framework-captured and `#[WithTelemetryChannel]` alike:

```yaml
flow_telemetry:
  channel_attribute_target: both   # both (default) | scope | signal
```

- **`scope`** — `log.channel` sits on the instrumentation scope. Filter by channel with an
  [`attribute_filtering`](#attribute_filtering) processor using `sources: [scope]`. Not present on individual records.
- **`signal`** — `log.channel` is merged into every emitted record, so it is visible per-record and filterable with the
  default `sources: [signal]` (the closest match to how Monolog stamps the channel on each record).
- **`both`** (default) — placed on the scope *and* every record, so it is filterable either way at the cost of storing
  the attribute on each record.

This only governs channels the bundle synthesizes; a channel you declare yourself under `loggers` opts out of synthesis,
so set `log.channel` explicitly on whichever of `attributes.scope` / `attributes.signal` you want.

> [!NOTE]
> `capture_framework_channels` rewrites the `logger` reference on framework services to their channel logger. A
> service-specific channel takes precedence over the global [`framework_logger`](#main-logger) redirect.

#### Coexisting with MonologBundle

You can run this bundle **alongside MonologBundle** — as long as `capture_framework_channels` stays `false` (the
default). The two are independent because they read different DI tags: framework capture reads Symfony's
`monolog.logger`, while the `#[WithTelemetryChannel]` attribute uses this bundle's own `flow.telemetry.channel` tag,
which Monolog never looks at. With the flag off, this bundle never touches `monolog.logger`, so:

- MonologBundle keeps full ownership of every framework channel (router, request, Doctrine, …) and the default `app`
  logger — uncontested.
- `#[WithTelemetryChannel('events')]` still scopes an individual class: it binds only that service's `LoggerInterface`
  (and native `Logger`) to the Flow telemetry channel logger. Because the binding fills the argument directly, it wins
  over Monolog's global `LoggerInterface` autowiring alias for that one class; every other class keeps resolving to
  Monolog.

What you **cannot** do is enable `capture_framework_channels` *and* keep MonologBundle: both passes then rewrite the
same
`monolog.logger`-tagged services, and the outcome depends on compiler-pass ordering. Ownership of the framework channels
is all-or-nothing:

- **Flow telemetry owns the framework channels** — `capture_framework_channels: true`, MonologBundle not installed.
- **Monolog owns the framework channels, Flow scopes specific classes** — `capture_framework_channels: false`,
  MonologBundle installed.

The `#[WithTelemetryChannel]` attribute works in both setups.

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
      propagation_style: link
    dbal:
      enabled: true
      log_sql: true
      max_sql_length: 500
    cache:
      enabled: true
      exclude_pools:
        - 'cache.system'
```
