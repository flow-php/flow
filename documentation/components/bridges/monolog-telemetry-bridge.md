---
package: flow-php/monolog-telemetry-bridge
---

# Monolog Telemetry Bridge

This bridge connects Monolog with the Flow PHP Telemetry library. You use Monolog as your logging interface, and
the logs go out through the telemetry system of Flow.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/monolog-telemetry-bridge.md).

## Overview

This bridge has a Monolog handler that sends log records to Flow Telemetry. With it, you can:

- Keep Monolog as your logging interface, and use the export features of Telemetry (for example console or OTLP)
- Keep detailed context data: context values become `context.*` attributes, extra values become `extra.*` attributes
- Handle exceptions automatically: throwables in the context go to the `setException()` method of Telemetry
- Map Monolog severity levels to Telemetry severities, with a mapping that you can change

## Basic Usage

The simplest integration needs only a few lines of code:

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

The bridge has four DSL functions for configuration and customization.

### telemetry_handler()

The main handler that sends Monolog logs to Flow Telemetry. This is the function that you use most.

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

Creates a converter that transforms a Monolog LogRecord to a Telemetry LogRecord. Use it for custom severity mapping or value normalization.

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

Creates a mapper that converts Monolog levels to Telemetry severities. Use it for a severity mapping that is different from the defaults.

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

### attribute_value_normalizer()

Creates a normalizer that converts arbitrary PHP values to types that Telemetry attributes accept.

```php
<?php

use function Flow\Bridge\Monolog\Telemetry\DSL\attribute_value_normalizer;

$normalizer = attribute_value_normalizer();
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

When a Monolog log record becomes a Telemetry log record, the data is mapped like this:

| Monolog Property | Telemetry Attribute  | Description                               |
|------------------|----------------------|-------------------------------------------|
| `message`        | `body`               | The log message body                      |
| `channel`        | `monolog.channel`    | The Monolog channel name                  |
| `level->name`    | `monolog.level_name` | The Monolog level name (for example INFO) |
| `context.*`      | `context.*`          | Prefixed context values                   |
| `extra.*`        | `extra.*`            | Prefixed extra values                     |

Example:

```php
<?php

$monolog->info('Order processed', [
    'order_id' => 12345,
    'amount' => 99.99,
    'customer' => ['id' => 1, 'name' => 'John'],
]);
```

The result is these Telemetry attributes:

- `monolog.channel` → `'channel'`
- `monolog.level_name` → `'INFO'`
- `context.order_id` → `12345`
- `context.amount` → `99.99`
- `context.customer` → `['id' => 1, 'name' => 'John']`

## Exception Handling

Throwables in the Monolog context go automatically to the `setException()` method of Telemetry. It creates structured exception attributes.

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

The exception is not stored as a regular `context.exception` attribute. Instead, the `setException()` method of Telemetry creates standard exception attributes that follow the OpenTelemetry conventions.

## Severity Mapping

By default, the 8 Monolog log levels map to the 6 Telemetry severity levels:

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

For a different behavior, override the default mapping:

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

The `telemetry_handler()` function accepts these parameters:

| Parameter   | Type                 | Default                    | Description                                     |
|-------------|----------------------|----------------------------|-------------------------------------------------|
| `logger`    | `Logger`             | (required)                 | The Flow Telemetry logger                       |
| `converter` | `LogRecordConverter` | `new LogRecordConverter()` | Converter for transforming log records          |
| `level`     | `Level`              | `Level::Debug`             | Minimum logging level to handle                 |
| `bubble`    | `bool`               | `true`                     | Whether messages bubble to other handlers       |

Level filtering example:

```php
<?php

use Monolog\Level;

// Only forward WARNING and above to Telemetry
$handler = telemetry_handler($logger, level: Level::Warning);
```

Disable bubbling example:

```php
<?php

// Don't let logs bubble to subsequent handlers
$handler = telemetry_handler($logger, bubble: false);
```

## Complete Example

This complete example shows the integration with OTLP export:

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

You can use multiple Monolog channels. Each one sends to the same Telemetry logger:

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

The `monolog.channel` attribute in Telemetry shows the channel of each log.
