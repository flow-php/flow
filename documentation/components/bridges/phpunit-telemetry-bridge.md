---
package: flow-php/phpunit-telemetry-bridge
---

# PHPUnit Telemetry Bridge

A PHPUnit extension that collects test suite telemetry. It exports the telemetry to each OTLP-compatible backend,
for example OpenTelemetry Collector, Grafana Alloy, Honeycomb, Datadog or Jaeger.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read
the [installation page](/documentation/installation/packages/phpunit-telemetry-bridge.md).

## Configuration

Add the extension to your `phpunit.xml.dist`:

```xml

<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="service_name" value="my-test-suite"/>
        <parameter name="resource_attributes" value="service.version=1.0.0,deployment.environment.name=ci"/>
        <parameter name="endpoint" value="http://localhost:4318"/>
        <parameter name="emit_traces" value="true"/>
        <parameter name="emit_metrics" value="true"/>
        <parameter name="emit_test_spans" value="true"/>
        <parameter name="emit_test_case_spans" value="true"/>
        <parameter name="memory_real_usage" value="false"/>
    </bootstrap>
</extensions>
```

## Configuration Parameters

### Shared

| Parameter              | Environment variable                     | Default                 | Description                                                                                                                                                          |
|------------------------|------------------------------------------|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `service_name`         | `FLOW_PHPUNIT_OTEL_SERVICE_NAME`         | `phpunit`               | Service name reported in telemetry data                                                                                                                              |
| `resource_attributes`  | `FLOW_PHPUNIT_OTEL_RESOURCE_ATTRIBUTES`  | -                       | Extra resource attributes as `name=value,name2=value2` (URL-encode commas/spaces). Merged last, so they override detected attributes and can override `service.name` |
| `transport`            | `FLOW_PHPUNIT_OTEL_TRANSPORT`            | `curl`                  | Transport type: `curl`, `grpc`, or `stream`                                                                                                                          |
| `endpoint`             | `FLOW_PHPUNIT_OTEL_ENDPOINT`             | `http://localhost:4318` | OTLP endpoint URL (curl/grpc) or destination path (stream)                                                                                                           |
| `headers`              | `FLOW_PHPUNIT_OTEL_HEADERS`              | -                       | Additional headers (read [Authentication](#authentication))                                                                                                          |
| `emit_traces`          | `FLOW_PHPUNIT_OTEL_EMIT_TRACES`          | `true`                  | Enable/disable trace emission                                                                                                                                        |
| `emit_metrics`         | `FLOW_PHPUNIT_OTEL_EMIT_METRICS`         | `true`                  | Enable/disable metric emission                                                                                                                                       |
| `emit_test_spans`      | `FLOW_PHPUNIT_OTEL_EMIT_TEST_SPANS`      | `true`                  | Create individual spans for each test                                                                                                                                |
| `emit_test_case_spans` | `FLOW_PHPUNIT_OTEL_EMIT_TEST_CASE_SPANS` | `true`                  | Create spans for test case classes                                                                                                                                   |
| `memory_real_usage`    | `FLOW_PHPUNIT_OTEL_MEMORY_REAL_USAGE`    | `false`                 | Report total system-allocated memory (`real_usage=true`) instead of emalloc usage for memory metrics/attributes                                                      |
| `batch_size`           | `FLOW_PHPUNIT_OTEL_BATCH_SIZE`           | `512`                   | Items per batch for span/metric/log batching processors                                                                                                              |
| `shutdown_timeout_ms`  | `FLOW_PHPUNIT_OTEL_SHUTDOWN_TIMEOUT_MS`  | `5000`                  | Wall-clock budget in ms for draining pending requests at shutdown (curl/grpc)                                                                                        |
| `error_handler`        | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER`        | `error_log`             | How telemetry errors are reported (read [Error Handlers](#error-handlers))                                                                                           |

### Curl transport (`transport=curl`)

Timeouts are in milliseconds. The defaults expect a local collector on loopback / sidecar. `timeout_ms` is the
deadline of each request. `shutdown_timeout_ms` (shared, read above) is a wall-clock budget, enforced only at
shutdown. You can therefore keep `timeout_ms` tight for steady-state, and the graceful exit does not freeze.

| Parameter                 | Environment variable                        | Default | Description                                        |
|---------------------------|---------------------------------------------|---------|----------------------------------------------------|
| `curl_timeout_ms`         | `FLOW_PHPUNIT_OTEL_CURL_TIMEOUT_MS`         | `250`   | Total request deadline in milliseconds             |
| `curl_connect_timeout_ms` | `FLOW_PHPUNIT_OTEL_CURL_CONNECT_TIMEOUT_MS` | `250`   | TCP/TLS connect deadline in milliseconds           |
| `curl_compression`        | `FLOW_PHPUNIT_OTEL_CURL_COMPRESSION`        | `false` | Enable automatic response decompression            |
| `curl_follow_redirects`   | `FLOW_PHPUNIT_OTEL_CURL_FOLLOW_REDIRECTS`   | `true`  | Follow HTTP redirects                              |
| `curl_max_redirects`      | `FLOW_PHPUNIT_OTEL_CURL_MAX_REDIRECTS`      | `3`     | Maximum number of redirects to follow              |
| `curl_proxy`              | `FLOW_PHPUNIT_OTEL_CURL_PROXY`              | -       | Proxy server URL (for example `http://proxy:8080`) |
| `curl_ssl_verify_peer`    | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_PEER`    | `true`  | Verify SSL peer certificate                        |
| `curl_ssl_verify_host`    | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_HOST`    | `true`  | Verify SSL host name                               |
| `curl_ssl_cert_path`      | `FLOW_PHPUNIT_OTEL_CURL_SSL_CERT_PATH`      | -       | Path to client SSL certificate                     |
| `curl_ssl_key_path`       | `FLOW_PHPUNIT_OTEL_CURL_SSL_KEY_PATH`       | -       | Path to client SSL private key                     |
| `curl_ca_info_path`       | `FLOW_PHPUNIT_OTEL_CURL_CA_INFO_PATH`       | -       | Path to CA certificate bundle                      |
| `curl_serializer`         | `FLOW_PHPUNIT_OTEL_CURL_SERIALIZER`         | `json`  | Payload serializer: `json` or `protobuf`           |

### gRPC transport (`transport=grpc`)

Needs the `grpc` PHP extension and the `google/protobuf` package. The payload is always protobuf, as the OTLP/gRPC
specification says. gRPC has no separate connect timeout. `grpc_timeout_ms` is the deadline of each call, for DNS,
connect, send and receive together.

| Parameter         | Environment variable                | Default | Description                       |
|-------------------|-------------------------------------|---------|-----------------------------------|
| `grpc_timeout_ms` | `FLOW_PHPUNIT_OTEL_GRPC_TIMEOUT_MS` | `250`   | Per-call deadline in milliseconds |
| `grpc_insecure`   | `FLOW_PHPUNIT_OTEL_GRPC_INSECURE`   | `true`  | Use insecure channel credentials  |

### Stream transport (`transport=stream`)

Writes JSONL telemetry to a file or a `php://` stream wrapper, not to an OTLP collector. The format follows the
[OTLP File Exporter spec](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/) here. The `endpoint`
parameter holds the destination path. Use it for local debugging, or for pipelines that read JSONL from disk / stdout.

| Parameter                   | Environment variable                          | Default | Description                                                |
|-----------------------------|-----------------------------------------------|---------|------------------------------------------------------------|
| `stream_file_permissions`   | `FLOW_PHPUNIT_OTEL_STREAM_FILE_PERMISSIONS`   | `0644`  | Permissions for newly created files (ignored for `php://`) |
| `stream_create_directories` | `FLOW_PHPUNIT_OTEL_STREAM_CREATE_DIRECTORIES` | `true`  | Create the missing parent directories of the destination   |

## Error Handlers

Internal telemetry errors (exporter failures, transport timeouts) go to a configurable error handler. They are not
thrown into the test runner. Pick a handler with the `error_handler` parameter. All options use the same
`error_handler_*` parameter family.

| Type         | Description                                                   |
|--------------|---------------------------------------------------------------|
| `error_log`  | Default. Writes through `error_log()` of PHP (stderr in CLI). |
| `noop`       | Silently discards each Throwable.                             |
| `stream`     | Appends to a file path or `php://` stream wrapper.            |
| `syslog`     | Local syslog via `openlog` / `syslog` / `closelog`.           |
| `udp_syslog` | RFC 5424 syslog frames over UDP to a remote collector.        |

Parameters of different handler types together throw `InvalidArgumentException` at boot.

### `error_log` (default)

| Parameter                       | Environment variable                              | Default            | Description                                                   |
|---------------------------------|---------------------------------------------------|--------------------|---------------------------------------------------------------|
| `error_handler_message_type`    | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_TYPE`    | `operating_system` | `operating_system` (0), `email` (1), `file` (3), `sapi` (4)   |
| `error_handler_expand_newlines` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_EXPAND_NEWLINES` | `false`            | Emit one `error_log()` call per line of the formatted message |
| `error_handler_message_prefix`  | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_PREFIX`  | `[flow-telemetry]` | Prefix added to the start of each message                     |

### `noop`

No parameters. Use it to keep internal telemetry failures silent, for example in CI runs without a collector.

### `stream`

Appends formatted Throwables (one per line) to a destination. The handle opens lazily at the first error and is used again after that.

| Parameter                          | Environment variable                                 | Default            | Description                                                               |
|------------------------------------|------------------------------------------------------|--------------------|---------------------------------------------------------------------------|
| `error_handler_destination`        | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_DESTINATION`        | -                  | File path or a stream such as `php://stdout` or `php://stderr` (required) |
| `error_handler_file_permissions`   | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FILE_PERMISSIONS`   | `0644`             | Permissions for newly created files (ignored for `php://`)                |
| `error_handler_create_directories` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_CREATE_DIRECTORIES` | `true`             | Create the missing parent directories of the destination                  |
| `error_handler_message_prefix`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_MESSAGE_PREFIX`     | `[flow-telemetry]` | Prefix added to the start of each line                                    |

### `syslog`

| Parameter                | Environment variable                       | Default          | Description                                                          |
|--------------------------|--------------------------------------------|------------------|----------------------------------------------------------------------|
| `error_handler_ident`    | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_IDENT`    | `flow-telemetry` | Syslog identity tag                                                  |
| `error_handler_facility` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FACILITY` | `user`           | RFC 5424 facility (for example `user`, `local0`..`local7`, `mail`)   |
| `error_handler_log_opts` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_LOG_OPTS` | `1` (`LOG_PID`)  | Bitmask of `LOG_*` flags passed to `openlog()`                       |
| `error_handler_severity` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_SEVERITY` | `error`          | RFC 5424 severity (for example `error`, `warning`, `notice`, `info`) |

### `udp_syslog`

Sends RFC 5424 syslog frames over UDP. Use it to send test run errors to a central log aggregator.

| Parameter                | Environment variable                       | Default          | Description                   |
|--------------------------|--------------------------------------------|------------------|-------------------------------|
| `error_handler_host`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_HOST`     | -                | Remote syslog host (required) |
| `error_handler_port`     | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_PORT`     | `514`            | Remote syslog port            |
| `error_handler_ident`    | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_IDENT`    | `flow-telemetry` | Syslog identity tag           |
| `error_handler_facility` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_FACILITY` | `user`           | RFC 5424 facility             |
| `error_handler_severity` | `FLOW_PHPUNIT_OTEL_ERROR_HANDLER_SEVERITY` | `error`          | RFC 5424 severity             |

## Authentication

Configure OTLP endpoints that need authentication through the `headers` parameter. Examples are Grafana Alloy with
Bearer auth, Honeycomb with `x-honeycomb-team`, and vendor tenant headers.

Headers use the OpenTelemetry specification format: comma-separated `name=value` pairs, with URL-encoded values. Commas,
spaces and equal signs inside values therefore do not collide with the delimiter:

```xml

<parameter name="endpoint" value="https://alloy.example.com:4318"/>
<parameter name="headers" value="Authorization=Bearer%20xxx,X-Scope-OrgID=tenant-1"/>
```

An invalid header format (missing `=`, empty name) throws `InvalidArgumentException` at the boot of the extension.

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

Each parameter has an environment variable counterpart (read the tables above).

Precedence: environment variable > `<parameter>` in `phpunit.xml` > default. An environment variable with an empty
string counts as unset.

Lookup order: `$_ENV` → `$_SERVER` → `getenv()`. Symfony DotEnv and `vlucas/phpdotenv` fill `$_ENV` / `$_SERVER`,
but they do not always call `putenv()`. Values that they load still work in the same way as shell environment
variables.

> [!IMPORTANT]
> To set boolean environment variables through the `<env>` directive of PHPUnit in `phpunit.xml.dist`, add
> `verbatim="true"`. Without it, PHPUnit casts the bare strings `"true"` / `"false"` to PHP booleans. `putenv()` then
> turns them into the strings `"1"` / `""`. The resolver treats the empty string as unset and uses the default.
>
> ```xml
> <env name="FLOW_PHPUNIT_OTEL_EMIT_TEST_SPANS" value="false" verbatim="true"/>
> ```
>
> This applies only to `<env>` in PHPUnit XML. Real shell environment variables and `.env` entries are already plain strings.

Typical usage, with the credentials outside of version control:

```bash
export FLOW_PHPUNIT_OTEL_ENDPOINT="https://alloy.example.com:4318"
export FLOW_PHPUNIT_OTEL_HEADERS="Authorization=Bearer%20${ALLOY_TOKEN}"

./vendor/bin/phpunit
```

## Deprecation: `otel_collector_url`

The `otel_collector_url` parameter and its `FLOW_PHPUNIT_OTEL_COLLECTOR_URL` environment variable are deprecated, and
they trigger `E_USER_DEPRECATED`. They still work as an alias for `endpoint` with `transport=curl`.

The deprecated parameter together with one of the new-shape parameters throws `InvalidArgumentException`. When you
switch, migrate fully.

## Features

### Traces

When traces are enabled, the extension creates spans for:

- Test suite runs (root span)
- Individual test suites
- Test case classes (optional)
- Individual tests (optional)

Each span has attributes such as:

- `test.suite.name`: test suite name
- `test.suite.run.status`: suite outcome (success, failure, skipped)
- `test.case.name`: test name
- `test.case.result.status`: test result status (passed, failed, errored, skipped, incomplete)
- `flow.phpunit.test.id`: test identifier
- `flow.phpunit.test.class`: test class name
- `flow.phpunit.test.method`: test method name
- `flow.phpunit.test.memory.peak` / `flow.phpunit.test.memory.delta`: memory usage in bytes

### Metrics

When metrics are enabled, the extension records:

- `flow.phpunit.suite.duration`: histogram of suite execution time (seconds)
- `flow.phpunit.suite.test_count`: counter of tests per suite (`{test}`)
- `flow.phpunit.test.duration`: histogram of individual test execution time (seconds)
- `flow.phpunit.test.count`: counter of tests by status (`{test}`)
- `flow.phpunit.test.memory.peak` / `flow.phpunit.test.memory.delta`: memory histograms (`By`)

## Running with Docker Compose

To see the test telemetry, run an OTEL collector with a backend such as Jaeger:

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
