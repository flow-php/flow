# PHPUnit Telemetry Bridge

PHPUnit extension allowing to collect test suite telemetry and export it to any OTLP-compatible backend (OpenTelemetry
Collector, Grafana Alloy, Honeycomb, Datadog, Jaeger, etc.).

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/phpunit-telemetry-bridge)
- [➡️ Installation](/documentation/installation/packages/phpunit-telemetry-bridge.md)
- [GitHub](https://github.com/flow-php/phpunit-telemetry-bridge)
- [API Reference](/documentation/api/bridge/phpunit/telemetry)

[TOC]

## Installation

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/phpunit-telemetry-bridge.md).

## Configuration

Add the extension to your `phpunit.xml.dist`:

```xml

<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="service_name" value="my-test-suite"/>
        <parameter name="endpoint" value="http://localhost:4318"/>
        <parameter name="emit_traces" value="true"/>
        <parameter name="emit_metrics" value="true"/>
        <parameter name="emit_test_spans" value="true"/>
        <parameter name="emit_test_case_spans" value="true"/>
    </bootstrap>
</extensions>
```

## Configuration Parameters

### Shared

| Parameter              | Environment variable                     | Default                 | Description                                                    |
|------------------------|------------------------------------------|-------------------------|----------------------------------------------------------------|
| `service_name`         | `FLOW_PHPUNIT_OTEL_SERVICE_NAME`         | `phpunit`               | Service name reported in telemetry data                        |
| `transport`            | `FLOW_PHPUNIT_OTEL_TRANSPORT`            | `curl`                  | Transport type: `curl`, `grpc`, or `stream`                    |
| `endpoint`             | `FLOW_PHPUNIT_OTEL_ENDPOINT`             | `http://localhost:4318` | OTLP endpoint URL (curl/grpc) or destination path (stream)     |
| `headers`              | `FLOW_PHPUNIT_OTEL_HEADERS`              | —                       | Additional headers (see [Authentication](#authentication))     |
| `emit_traces`          | `FLOW_PHPUNIT_OTEL_EMIT_TRACES`          | `true`                  | Enable/disable trace emission                                  |
| `emit_metrics`         | `FLOW_PHPUNIT_OTEL_EMIT_METRICS`         | `true`                  | Enable/disable metric emission                                 |
| `emit_test_spans`      | `FLOW_PHPUNIT_OTEL_EMIT_TEST_SPANS`      | `true`                  | Create individual spans for each test                          |
| `emit_test_case_spans` | `FLOW_PHPUNIT_OTEL_EMIT_TEST_CASE_SPANS` | `true`                  | Create spans for test case classes                             |
| `batch_size`           | `FLOW_PHPUNIT_OTEL_BATCH_SIZE`           | `512`                   | Items per batch for span/metric/log batching processors        |
| `shutdown_timeout_ms`  | `FLOW_PHPUNIT_OTEL_SHUTDOWN_TIMEOUT_MS`  | `5000`                  | Wall-clock budget in ms for draining pending requests at shutdown (curl/grpc) |
| `error_handler`        | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER`        | `error_log`             | How telemetry errors are surfaced (see [Error Handlers](#error-handlers)) |

### Curl transport (`transport=curl`)

Timeouts are in **milliseconds** (defaults assume a local collector on loopback / sidecar). `timeout_ms` is the
per-request deadline; `shutdown_timeout_ms` (shared, see above) is a wall-clock budget enforced at shutdown only —
it lets you keep `timeout_ms` tight for steady-state without freezing graceful exit.

| Parameter                  | Environment variable                         | Default | Description                                                  |
|----------------------------|----------------------------------------------|---------|--------------------------------------------------------------|
| `curl_timeout_ms`          | `FLOW_PHPUNIT_OTEL_CURL_TIMEOUT_MS`          | `250`   | Total request deadline in milliseconds                       |
| `curl_connect_timeout_ms`  | `FLOW_PHPUNIT_OTEL_CURL_CONNECT_TIMEOUT_MS`  | `250`   | TCP/TLS connect deadline in milliseconds                     |
| `curl_compression`         | `FLOW_PHPUNIT_OTEL_CURL_COMPRESSION`         | `false` | Enable automatic response decompression                      |
| `curl_follow_redirects`    | `FLOW_PHPUNIT_OTEL_CURL_FOLLOW_REDIRECTS`    | `true`  | Follow HTTP redirects                                        |
| `curl_max_redirects`       | `FLOW_PHPUNIT_OTEL_CURL_MAX_REDIRECTS`       | `3`     | Maximum number of redirects to follow                        |
| `curl_proxy`               | `FLOW_PHPUNIT_OTEL_CURL_PROXY`               | —       | Proxy server URL (e.g. `http://proxy:8080`)                  |
| `curl_ssl_verify_peer`     | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_PEER`     | `true`  | Verify SSL peer certificate                                  |
| `curl_ssl_verify_host`     | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_HOST`     | `true`  | Verify SSL host name                                         |
| `curl_ssl_cert_path`       | `FLOW_PHPUNIT_OTEL_CURL_SSL_CERT_PATH`       | —       | Path to client SSL certificate                               |
| `curl_ssl_key_path`        | `FLOW_PHPUNIT_OTEL_CURL_SSL_KEY_PATH`        | —       | Path to client SSL private key                               |
| `curl_ca_info_path`        | `FLOW_PHPUNIT_OTEL_CURL_CA_INFO_PATH`        | —       | Path to CA certificate bundle                                |
| `curl_serializer`          | `FLOW_PHPUNIT_OTEL_CURL_SERIALIZER`          | `json`  | Payload serializer: `json` or `protobuf`                     |

### gRPC transport (`transport=grpc`)

Requires the `grpc` PHP extension and the `google/protobuf` package. Payload is always protobuf (per OTLP/gRPC spec).
gRPC has no separate connect timeout — `grpc_timeout_ms` is the per-call deadline that bounds DNS, connect, send and
receive together.

| Parameter         | Environment variable                | Default | Description                                            |
|-------------------|-------------------------------------|---------|--------------------------------------------------------|
| `grpc_timeout_ms` | `FLOW_PHPUNIT_OTEL_GRPC_TIMEOUT_MS` | `250`   | Per-call deadline in milliseconds                      |
| `grpc_insecure`   | `FLOW_PHPUNIT_OTEL_GRPC_INSECURE`   | `true`  | Use insecure channel credentials                       |

### Stream transport (`transport=stream`)

Writes JSONL telemetry to a file or `php://` stream wrapper instead of an OTLP collector
([OTLP File Exporter spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/)). The `endpoint`
parameter holds the destination path. Useful for local debugging or pipelines that scrape JSONL from disk / stdout.

| Parameter                  | Environment variable                         | Default | Description                                                  |
|----------------------------|----------------------------------------------|---------|--------------------------------------------------------------|
| `stream_file_permissions`  | `FLOW_PHPUNIT_OTEL_STREAM_FILE_PERMISSIONS`  | `0644`  | Permissions for newly created files (ignored for `php://`)   |
| `stream_create_directories`| `FLOW_PHPUNIT_OTEL_STREAM_CREATE_DIRECTORIES`| `true`  | Create parent directories of the destination if missing      |

## Error Handlers

Telemetry-internal errors (exporter failures, transport timeouts) are routed to a configurable error handler instead of
being thrown into the test runner. Pick one with the `error_handler` parameter; all options follow the same
`error_handler_*` parameter family.

| Type         | Description                                                    |
|--------------|----------------------------------------------------------------|
| `error_log`  | Default. Writes via PHP's `error_log()` (stderr in CLI).       |
| `noop`       | Silently discards every Throwable.                             |
| `stream`     | Appends to a file path or `php://` stream wrapper.             |
| `syslog`     | Local syslog via `openlog` / `syslog` / `closelog`.            |
| `udp_syslog` | RFC 5424 syslog frames over UDP to a remote collector.         |

Mixing parameters across handler types throws `InvalidArgumentException` at boot.

### `error_log` (default)

| Parameter                       | Environment variable                              | Default            | Description                                                  |
|---------------------------------|---------------------------------------------------|--------------------|--------------------------------------------------------------|
| `error_handler_message_type`    | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_TYPE`    | `operating_system` | `operating_system` (0), `email` (1), `file` (3), `sapi` (4)  |
| `error_handler_expand_newlines` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_EXPAND_NEWLINES` | `false`            | Emit one `error_log()` call per line of the formatted message |
| `error_handler_message_prefix`  | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_PREFIX`  | `[flow-telemetry]` | Prefix prepended to every message                            |

### `noop`

No parameters. Use when you want telemetry-internal failures to stay silent (e.g. CI runs where the collector is
intentionally absent).

### `stream`

Appends formatted Throwables (one per line) to a destination. The handle is opened lazily on first error and reused.

| Parameter                          | Environment variable                                 | Default            | Description                                                  |
|------------------------------------|------------------------------------------------------|--------------------|--------------------------------------------------------------|
| `error_handler_destination`        | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_DESTINATION`        | —                  | File path or `php://stdout`/`php://stderr`/etc. (required)   |
| `error_handler_file_permissions`   | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FILE_PERMISSIONS`   | `0644`             | Permissions for newly created files (ignored for `php://`)   |
| `error_handler_create_directories` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_CREATE_DIRECTORIES` | `true`             | Create parent directories of the destination if missing      |
| `error_handler_message_prefix`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_PREFIX`     | `[flow-telemetry]` | Prefix prepended to every line                               |

### `syslog`

| Parameter                  | Environment variable                         | Default          | Description                                                  |
|----------------------------|----------------------------------------------|------------------|--------------------------------------------------------------|
| `error_handler_ident`      | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_IDENT`      | `flow-telemetry` | Syslog identity tag                                          |
| `error_handler_facility`   | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FACILITY`   | `user`           | RFC 5424 facility (e.g. `user`, `local0`..`local7`, `mail`)  |
| `error_handler_log_opts`   | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_LOG_OPTS`   | `1` (`LOG_PID`)  | Bitmask of `LOG_*` flags passed to `openlog()`               |
| `error_handler_severity`   | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_SEVERITY`   | `error`          | RFC 5424 severity (e.g. `error`, `warning`, `notice`, `info`) |

### `udp_syslog`

Sends RFC 5424 syslog frames over UDP. Useful when test runs ship to a centralized log aggregator.

| Parameter                | Environment variable                       | Default          | Description                                                  |
|--------------------------|--------------------------------------------|------------------|--------------------------------------------------------------|
| `error_handler_host`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_HOST`     | —                | Remote syslog host (required)                                |
| `error_handler_port`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_PORT`     | `514`            | Remote syslog port                                           |
| `error_handler_ident`    | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_IDENT`    | `flow-telemetry` | Syslog identity tag                                          |
| `error_handler_facility` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FACILITY` | `user`           | RFC 5424 facility                                            |
| `error_handler_severity` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_SEVERITY` | `error`          | RFC 5424 severity                                            |

## Authentication

OTLP endpoints that require authentication (Grafana Alloy with Bearer auth, Honeycomb with `x-honeycomb-team`, vendor
tenant headers, etc.) are configured through the `headers` parameter.

Headers use the OpenTelemetry spec format: comma-separated `name=value` pairs, with values URL-encoded (so commas,
spaces and equal signs inside values don't collide with the delimiter):

```xml

<parameter name="endpoint" value="https://alloy.example.com:4318"/>
<parameter name="headers" value="Authorization=Bearer%20xxx,X-Scope-OrgID=tenant-1"/>
```

Invalid header format (missing `=`, empty name) will throw `InvalidArgumentException` when PHPUnit boots the extension.

## gRPC Transport

```xml

<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="transport" value="grpc"/>
        <parameter name="endpoint" value="otel.example.com:4317"/>
        <parameter name="headers" value="api-key=xxx"/>
        <parameter name="grpc_insecure" value="false"/>
        <parameter name="grpc_timeout_ms" value="2000"/>
    </bootstrap>
</extensions>
```

## Stream Transport

```xml

<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="transport" value="stream"/>
        <parameter name="endpoint" value="var/otel/test-suite.jsonl"/>
        <parameter name="error_handler" value="stream"/>
        <parameter name="error_handler_destination" value="var/otel/test-suite-errors.log"/>
    </bootstrap>
</extensions>
```

## Environment Variables

Every parameter has an env var counterpart (see the tables above).

**Precedence:** environment variable > `<parameter>` in `phpunit.xml` > default. Empty-string env vars are treated as unset.

**Lookup order:** `$_ENV` → `$_SERVER` → `getenv()`. Values loaded by Symfony DotEnv or `vlucas/phpdotenv`
(which populate `$_ENV` / `$_SERVER` but don't always call `putenv()`) are picked up the same as shell env vars.

> [!IMPORTANT]
> When setting boolean env vars through PHPUnit's `<env>` directive in `phpunit.xml.dist`, add `verbatim="true"` — otherwise
> PHPUnit casts the bare strings `"true"` / `"false"` to PHP booleans, which `putenv()` then stringifies to `"1"` / `""`, and
> the empty string is treated as unset by the resolver (falling back to the default).
>
> ```xml
> <env name="FLOW_PHPUNIT_OTEL_EMIT_TEST_SPANS" value="false" verbatim="true"/>
> ```
>
> This only applies to `<env>` in PHPUnit XML; real shell env vars and `.env` entries are already plain strings.

Typical usage — credentials stay out of version control:

```bash
export FLOW_PHPUNIT_OTEL_ENDPOINT="https://alloy.example.com:4318"
export FLOW_PHPUNIT_OTEL_HEADERS="Authorization=Bearer%20${ALLOY_TOKEN}"

./vendor/bin/phpunit
```

## Deprecation: `otel_collector_url`

The `otel_collector_url` parameter and its `FLOW_PHPUNIT_OTEL_COLLECTOR_URL` environment variable are deprecated and
trigger `E_USER_DEPRECATED`. They remain functional as an alias for `endpoint` with `transport=curl`.

Mixing the deprecated parameter with any of the new-shape parameters throws `InvalidArgumentException` — migrate
fully when you switch.

## Features

### Traces

When enabled, the extension creates spans for:

- Test suite runs (root span)
- Individual test suites
- Test case classes (optional)
- Individual tests (optional)

Each span includes attributes like:

- `test.suite` - Test suite name
- `test.id` - Test identifier
- `test.name` - Test name
- `test.class` - Test class name
- `test.method` - Test method name
- `test.status` - Test result status (passed, failed, errored, skipped, incomplete)

### Metrics

When enabled, the extension records:

- `phpunit.suite.duration` - Histogram of suite execution time
- `phpunit.suite.test_count` - Counter of tests per suite
- `phpunit.test.duration` - Histogram of individual test execution time
- `phpunit.test.count` - Counter of tests by status

## Running with Docker Compose

To visualize test telemetry, run an OTEL collector with a backend like Jaeger:

```yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector:latest
    ports:
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol/config.yaml

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686" # UI
```

Then run your tests:

```bash
./vendor/bin/phpunit
```

View the traces in Jaeger at `http://localhost:16686`.
