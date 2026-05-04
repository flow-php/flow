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
| `transport`            | `FLOW_PHPUNIT_OTEL_TRANSPORT`            | `curl`                  | Transport type: `curl` or `grpc`                               |
| `endpoint`             | `FLOW_PHPUNIT_OTEL_ENDPOINT`             | `http://localhost:4318` | OTLP endpoint URL (for `grpc` use host:port, e.g. `otel:4317`) |
| `headers`              | `FLOW_PHPUNIT_OTEL_HEADERS`              | —                       | Additional headers (see [Authentication](#authentication))     |
| `emit_traces`          | `FLOW_PHPUNIT_OTEL_EMIT_TRACES`          | `true`                  | Enable/disable trace emission                                  |
| `emit_metrics`         | `FLOW_PHPUNIT_OTEL_EMIT_METRICS`         | `true`                  | Enable/disable metric emission                                 |
| `emit_test_spans`      | `FLOW_PHPUNIT_OTEL_EMIT_TEST_SPANS`      | `true`                  | Create individual spans for each test                          |
| `emit_test_case_spans` | `FLOW_PHPUNIT_OTEL_EMIT_TEST_CASE_SPANS` | `true`                  | Create spans for test case classes                             |

### Curl transport (`transport=curl`)

| Parameter               | Environment variable                      | Default | Description                                 |
|-------------------------|-------------------------------------------|---------|---------------------------------------------|
| `curl_timeout`          | `FLOW_PHPUNIT_OTEL_CURL_TIMEOUT`          | `30`    | Request timeout in seconds                  |
| `curl_connect_timeout`  | `FLOW_PHPUNIT_OTEL_CURL_CONNECT_TIMEOUT`  | `10`    | Connection timeout in seconds               |
| `curl_compression`      | `FLOW_PHPUNIT_OTEL_CURL_COMPRESSION`      | `false` | Enable automatic response decompression     |
| `curl_follow_redirects` | `FLOW_PHPUNIT_OTEL_CURL_FOLLOW_REDIRECTS` | `true`  | Follow HTTP redirects                       |
| `curl_max_redirects`    | `FLOW_PHPUNIT_OTEL_CURL_MAX_REDIRECTS`    | `3`     | Maximum number of redirects to follow       |
| `curl_proxy`            | `FLOW_PHPUNIT_OTEL_CURL_PROXY`            | —       | Proxy server URL (e.g. `http://proxy:8080`) |
| `curl_ssl_verify_peer`  | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_PEER`  | `true`  | Verify SSL peer certificate                 |
| `curl_ssl_verify_host`  | `FLOW_PHPUNIT_OTEL_CURL_SSL_VERIFY_HOST`  | `true`  | Verify SSL host name                        |
| `curl_ssl_cert_path`    | `FLOW_PHPUNIT_OTEL_CURL_SSL_CERT_PATH`    | —       | Path to client SSL certificate              |
| `curl_ssl_key_path`     | `FLOW_PHPUNIT_OTEL_CURL_SSL_KEY_PATH`     | —       | Path to client SSL private key              |
| `curl_ca_info_path`     | `FLOW_PHPUNIT_OTEL_CURL_CA_INFO_PATH`     | —       | Path to CA certificate bundle               |
| `curl_serializer`       | `FLOW_PHPUNIT_OTEL_CURL_SERIALIZER`       | `json`  | Payload serializer: `json` or `protobuf`    |

### gRPC transport (`transport=grpc`)

Requires the `grpc` PHP extension and the `google/protobuf` package. Payload is always protobuf (per OTLP/gRPC spec).

| Parameter       | Environment variable              | Default | Description                      |
|-----------------|-----------------------------------|---------|----------------------------------|
| `grpc_insecure` | `FLOW_PHPUNIT_OTEL_GRPC_INSECURE` | `true`  | Use insecure channel credentials |

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

Mixing the deprecated parameter with any of the new-shape parameters (`transport`, `endpoint`, `headers`, `curl_*`,
`grpc_*`) throws `InvalidArgumentException` — migrate fully when you switch.

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
