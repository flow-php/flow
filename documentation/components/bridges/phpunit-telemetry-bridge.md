# PHPUnit Telemetry Bridge

PHPUnit extension allowing to collect test suite telemetry and export to OTEL collector.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/phpunit-telemetry-bridge)
- [➡️ Installation](/documentation/installation/packages/phpunit-telemetry-bridge.md)
- [GitHub](https://github.com/flow-php/phpunit-telemetry-bridge)
- [API Reference](/documentation/api/bridge/phpunit/telemetry)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/phpunit-telemetry-bridge.md).

## Configuration

Add the extension to your `phpunit.xml.dist`:

```xml
<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="service_name" value="my-test-suite"/>
        <parameter name="otel_collector_url" value="http://localhost:4318"/>
        <parameter name="emit_traces" value="true"/>
        <parameter name="emit_metrics" value="true"/>
        <parameter name="emit_test_spans" value="false"/>
        <parameter name="emit_test_case_spans" value="false"/>
    </bootstrap>
</extensions>
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `service_name` | `phpunit` | Service name used in telemetry data |
| `otel_collector_url` | `http://localhost:4318` | OTLP HTTP endpoint URL |
| `emit_traces` | `true` | Enable/disable trace emission |
| `emit_metrics` | `true` | Enable/disable metric emission |
| `emit_test_spans` | `true` | Create individual spans for each test |
| `emit_test_case_spans` | `true` | Create spans for test case classes |

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
