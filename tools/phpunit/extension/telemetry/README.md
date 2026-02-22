# PHPUnit Telemetry Extension

A PHPUnit extension that exports test telemetry to an OpenTelemetry collector using Flow PHP's telemetry library.

## Installation

This extension is part of the Flow PHP monorepo and is automatically available when using the main `flow-php/flow`
package.

## Configuration

Add the extension to your `phpunit.xml`:

```xml
<extensions>
    <bootstrap class="Flow\Tool\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="service_name" value="my-project-tests"/>
        <parameter name="otel_collector_url" value="http://otel-collector:4318"/>
        <parameter name="emit_traces" value="true"/>
        <parameter name="emit_metrics" value="true"/>
        <parameter name="emit_test_spans" value="true"/>
        <parameter name="emit_test_case_spans" value="true"/>
    </bootstrap>
</extensions>
```

### Configuration Parameters

| Parameter              | Type   | Default                 | Description                                        |
|------------------------|--------|-------------------------|----------------------------------------------------|
| `service_name`         | string | `phpunit`               | Service name for telemetry resource                |
| `otel_collector_url`   | string | `http://localhost:4318` | OTEL collector HTTP endpoint                       |
| `emit_traces`          | bool   | `true`                  | Enable span export                                 |
| `emit_metrics`         | bool   | `true`                  | Enable metric export                               |
| `emit_test_spans`      | bool   | `true`                  | Emit spans for individual test methods             |
| `emit_test_case_spans` | bool   | `true`                  | Emit spans for test cases (classes)                |

### Reducing Telemetry Volume

For large test suites, you may want to reduce telemetry volume by disabling granular spans:

```xml
<extensions>
    <bootstrap class="Flow\Tool\PHPUnit\Telemetry\TelemetryExtension">
        <parameter name="service_name" value="my-project-tests"/>
        <!-- Only emit root test suite span -->
        <parameter name="emit_test_spans" value="false"/>
        <parameter name="emit_test_case_spans" value="false"/>
    </bootstrap>
</extensions>
```

**Span hierarchy and what each option controls:**

```
Test Suite Run (root span)              <- Always emitted
  └── etl-unit (named testsuite)        <- Always emitted (from phpunit.xml testsuites)
        └── DateIntervalFunctionsTest   <- Controlled by emit_test_case_spans
              └── test_example          <- Controlled by emit_test_spans
```

## Telemetry Data

### Spans

The extension creates a hierarchical span structure:

```
Test Suite Run (root span)
  └── Test Suite / Class (span)
        └── Test Method (span)
```

#### Test Method Span Attributes

| Attribute           | Description                                      |
|---------------------|--------------------------------------------------|
| `test.class`        | Fully qualified class name                       |
| `test.method`       | Method name                                      |
| `test.status`       | passed / failed / errored / skipped / incomplete |
| `test.duration_ms`  | Execution time in milliseconds                   |
| `test.suite`        | Suite name                                       |
| `exception.message` | Error message (if failed/errored)                |

### Metrics

| Metric                     | Type           | Labels        |
|----------------------------|----------------|---------------|
| `phpunit.test.duration`    | histogram (ms) | `test.status` |
| `phpunit.test.count`       | counter        | `test.status` |
| `phpunit.suite.duration`   | histogram (ms) | `test.suite`  |
| `phpunit.suite.test_count` | counter        | `test.suite`  |

## Error Handling

The extension is designed to never break your tests. All telemetry operations are wrapped in try-catch blocks with
silent failure. If telemetry fails for any reason (network issues, configuration errors, etc.), your tests will continue
to run normally.
