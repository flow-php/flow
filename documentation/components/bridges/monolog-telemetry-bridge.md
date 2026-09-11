---
package: flow-php/monolog-telemetry-bridge
---

# Monolog Telemetry Bridge

Flow Monolog Telemetry Bridge connects Monolog with Flow PHP Telemetry library, allowing you to use Monolog as your
logging interface while exporting logs through Flow's telemetry system.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/monolog-telemetry-bridge.md).

## Overview

This bridge provides a Monolog handler that forwards log records to Flow Telemetry. It allows you to:

- Keep using Monolog as your logging interface while leveraging Telemetry's export capabilities (console, OTLP, etc.)
- Preserve rich context data - context values become `context.*` attributes, extra values become `extra.*` attributes
- Automatic exception handling - Throwables in context are detected and passed to Telemetry's `setException()` method
- Map Monolog severity levels to Telemetry severities with customizable mapping

## Basic Usage

The simplest integration requires just a few lines of code:

```php
<?php

use Monolog\Logger as MonologLogger;
use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;
use function Flow\Telemetry\DSL\telemetry;

$telemetry = telemetry();
$logger = $telemetry->logger('my-app');

$monolog = new MonologLogger('channel');
$monolog->pushHandler(telemetry_handler($logger));

$monolog->info('User logged in', ['user_id' => 123]);
// → Forwarded to Flow Telemetry with INFO severity
```

## DSL Functions

The bridge provides four DSL functions for configuration and customization.

### telemetry_handler()

The main handler that forwards Monolog logs to Flow Telemetry. This is the primary function you'll use.

```php
<?php

use Monolog\Level;
use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;

// Basic usage
$handler = telemetry_handler($logger);

// With custom minimum level and bubbling
$handler = telemetry_handler(
    logger: $logger,
    level: Level::Warning,  // Only handle WARNING and above
    bubble: false,          // Don't bubble to other handlers
);
```

### log_record_converter()

Creates a converter for transforming Monolog LogRecord to Telemetry LogRecord. Use this when you need custom severity mapping or value normalization.

```php
<?php

use function Flow\Bridge\Monolog\Telemetry\DSL\{log_record_converter, severity_mapper, telemetry_handler};

$converter = log_record_converter(
    severityMapper: severity_mapper([
        Level::Debug->value => Severity::TRACE,
    ])
);

$handler = telemetry_handler($logger, $converter);
```

### severity_mapper()

Creates a mapper for converting Monolog levels to Telemetry severities. Useful when you need different severity mapping than the defaults.

```php
<?php

use Monolog\Level;
use Flow\Telemetry\Logger\Severity;
use function Flow\Bridge\Monolog\Telemetry\DSL\severity_mapper;

// With default mapping
$mapper = severity_mapper();

// With custom mapping
$mapper = severity_mapper([
    Level::Debug->value => Severity::DEBUG,
    Level::Info->value => Severity::INFO,
    Level::Notice->value => Severity::WARN,  // Custom: NOTICE → WARN instead of INFO
    Level::Warning->value => Severity::WARN,
    Level::Error->value => Severity::ERROR,
    Level::Critical->value => Severity::FATAL,
    Level::Alert->value => Severity::FATAL,
    Level::Emergency->value => Severity::FATAL,
]);
```

### value_normalizer()

Creates a normalizer for converting arbitrary PHP values to types acceptable by Telemetry attributes.

```php
<?php

use function Flow\Bridge\Monolog\Telemetry\DSL\value_normalizer;

$normalizer = value_normalizer();
$normalized = $normalizer->normalize($value);
```

The normalizer handles:

| Input Type                          | Output                           |
|-------------------------------------|----------------------------------|
| `null`                              | `'null'` string                  |
| Scalars (string, int, float, bool)  | Unchanged                        |
| `DateTimeInterface`                 | Unchanged                        |
| `Throwable`                         | Unchanged                        |
| Arrays                              | Recursively normalized           |
| Objects with `__toString()`         | String cast                      |
| Objects without `__toString()`      | Class name                       |
| Other types                         | `get_debug_type()` result        |

## Context and Attributes

When a Monolog log record is converted to a Telemetry log record, the data is mapped as follows:

| Monolog Property | Telemetry Attribute     | Description                        |
|------------------|-------------------------|------------------------------------|
| `message`        | `body`                  | The log message body               |
| `channel`        | `monolog.channel`       | The Monolog channel name           |
| `level->name`    | `monolog.level_name`    | The Monolog level name (e.g., INFO)|
| `context.*`      | `context.*`             | Prefixed context values            |
| `extra.*`        | `extra.*`               | Prefixed extra values              |

**Example:**

```php
<?php

$monolog->info('Order processed', [
    'order_id' => 12345,
    'amount' => 99.99,
    'customer' => ['id' => 1, 'name' => 'John'],
]);
```

Results in Telemetry attributes:

- `monolog.channel` → `'channel'`
- `monolog.level_name` → `'INFO'`
- `context.order_id` → `12345`
- `context.amount` → `99.99`
- `context.customer` → `['id' => 1, 'name' => 'John']`

## Exception Handling

Throwables in the Monolog context are automatically detected and passed to Telemetry's `setException()` method, creating structured exception attributes.

```php
<?php

try {
    // ... code that throws
} catch (\Exception $e) {
    $monolog->error('Payment failed', [
        'order_id' => 123,
        'exception' => $e,  // Automatically handled via setException()
    ]);
}
```

The exception is not stored as a regular `context.exception` attribute. Instead, Telemetry's `setException()` method is called, which creates standardized exception attributes following OpenTelemetry conventions.

## Severity Mapping

By default, Monolog's 8 log levels are mapped to Telemetry's 6 severity levels:

| Monolog Level | Flow Telemetry Severity |
|---------------|-------------------------|
| DEBUG         | DEBUG                   |
| INFO          | INFO                    |
| NOTICE        | INFO                    |
| WARNING       | WARN                    |
| ERROR         | ERROR                   |
| CRITICAL      | FATAL                   |
| ALERT         | FATAL                   |
| EMERGENCY     | FATAL                   |

### Custom Severity Mapping

You can override the default mapping when you need different behavior:

```php
<?php

use Monolog\Level;
use Flow\Telemetry\Logger\Severity;
use function Flow\Bridge\Monolog\Telemetry\DSL\{log_record_converter, severity_mapper, telemetry_handler};

// Map NOTICE to WARN instead of INFO
$converter = log_record_converter(
    severityMapper: severity_mapper([
        Level::Debug->value => Severity::DEBUG,
        Level::Info->value => Severity::INFO,
        Level::Notice->value => Severity::WARN,
        Level::Warning->value => Severity::WARN,
        Level::Error->value => Severity::ERROR,
        Level::Critical->value => Severity::FATAL,
        Level::Alert->value => Severity::FATAL,
        Level::Emergency->value => Severity::FATAL,
    ])
);

$monolog->pushHandler(telemetry_handler($logger, $converter));
```

## Configuration Options

The `telemetry_handler()` function accepts the following parameters:

| Parameter   | Type                 | Default                    | Description                                     |
|-------------|----------------------|----------------------------|-------------------------------------------------|
| `logger`    | `Logger`             | (required)                 | The Flow Telemetry logger                       |
| `converter` | `LogRecordConverter` | `new LogRecordConverter()` | Converter for transforming log records          |
| `level`     | `Level`              | `Level::Debug`             | Minimum logging level to handle                 |
| `bubble`    | `bool`               | `true`                     | Whether messages should bubble to other handlers|

**Level filtering example:**

```php
<?php

use Monolog\Level;

// Only forward WARNING and above to Telemetry
$handler = telemetry_handler($logger, level: Level::Warning);
```

**Disable bubbling example:**

```php
<?php

// Don't let logs bubble to subsequent handlers
$handler = telemetry_handler($logger, bubble: false);
```

## Complete Example

Here's a complete example showing integration with OTLP export:

```php
<?php

use Monolog\Logger as MonologLogger;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use function Flow\Telemetry\DSL\{
    telemetry,
    resource,
    batching_log_processor,
};
use Flow\Telemetry\Provider\Clock\SystemClock;
use function Flow\Bridge\Telemetry\OTLP\DSL\{
    otlp_curl_transport,
    otlp_json_serializer,
    otlp_exporter,
    otlp_logger_provider,
};
use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;

// Set up Telemetry with OTLP export
$resource = resource([
    'service.name' => 'order-service',
    'service.version' => '1.0.0',
    'deployment.environment' => 'production',
]);

$transport = otlp_curl_transport(
    endpoint: 'http://otel-collector:4318',
    serializer: otlp_json_serializer(),
);

$telemetry = telemetry(
    $resource,
    loggerProvider: otlp_logger_provider(
        batching_log_processor(otlp_exporter($transport)),
        new SystemClock(),
    ),
);

$telemetry->registerShutdownFunction();

// Create Monolog logger with Telemetry handler
$logger = $telemetry->logger('order-service');
$monolog = new MonologLogger('orders');
$monolog->pushHandler(telemetry_handler($logger));

// Optionally, also log to a file
$monolog->pushHandler(new StreamHandler('/var/log/app.log', Level::Debug));

// Use Monolog as usual
$monolog->info('Order created', ['order_id' => 12345, 'amount' => 99.99]);
$monolog->warning('Inventory low', ['product_id' => 789, 'remaining' => 5]);

try {
    // ... payment processing
} catch (\Exception $e) {
    $monolog->error('Payment failed', [
        'order_id' => 12345,
        'exception' => $e,
    ]);
}
```

### Multiple Channels

You can use multiple Monolog channels, each forwarding to the same Telemetry logger:

```php
<?php

$telemetryLogger = $telemetry->logger('my-app');

$ordersLog = new MonologLogger('orders');
$ordersLog->pushHandler(telemetry_handler($telemetryLogger));

$paymentsLog = new MonologLogger('payments');
$paymentsLog->pushHandler(telemetry_handler($telemetryLogger));

$ordersLog->info('Order placed');    // monolog.channel = 'orders'
$paymentsLog->info('Payment received'); // monolog.channel = 'payments'
```

The `monolog.channel` attribute in Telemetry will reflect which channel each log came from.
