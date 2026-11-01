---
package: flow-php/psr3-telemetry-bridge
---

# PSR-3 Telemetry Bridge

This bridge has a [PSR-3](https://www.php-fig.org/psr/psr-3/) `LoggerInterface` implementation on Flow PHP
Telemetry. Each framework or library that depends on `Psr\Log\LoggerInterface` can write through this adapter. Its
records then become Telemetry log signals.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/psr3-telemetry-bridge.md).

## Overview

This bridge wraps `Flow\Telemetry\Logger\Logger` behind a PSR-3 `LoggerInterface`. It:

- Maps PSR-3 log levels to Telemetry severities (8 → 6, customizable).
- Does PSR-3 `{placeholder}` interpolation with the context array (only scalars and `Stringable`).
- Stores each context entry as a log record attribute under its raw key.
- Sends a `Throwable` under the `exception` key through `LogRecord::setException()`. This fills
  `exception.type`, `exception.message`, and `exception.stacktrace`.
- Throws `Psr\Log\InvalidArgumentException` for a `log()` call with an unknown level, as the specification says.

## Basic Usage

```php
<?php

use function Flow\Bridge\Psr3\Telemetry\DSL\psr3_telemetry_logger;
use function Flow\Telemetry\DSL\telemetry;

$telemetry = telemetry($resource);
$psrLogger = psr3_telemetry_logger($telemetry->logger('my-service'));

$psrLogger->info('User {user_id} logged in', ['user_id' => 123]);
// → body:       'User 123 logged in'
// → severity:   INFO
// → attributes: {user_id: 123}
```

The returned `TelemetryLogger` is a drop-in `Psr\Log\LoggerInterface`. You can give it to each thing that accepts a
PSR-3 logger (Symfony, Laravel, third-party libraries).

## Severity Mapping

The default mapping maps the eight PSR-3 levels to the six OTEL-aligned Telemetry severities:

| PSR-3 Level | Telemetry Severity |
|-------------|--------------------|
| `debug`     | `DEBUG`            |
| `info`      | `INFO`             |
| `notice`    | `INFO`             |
| `warning`   | `WARN`             |
| `error`     | `ERROR`            |
| `critical`  | `FATAL`            |
| `alert`     | `FATAL`            |
| `emergency` | `FATAL`            |

To override the mapping, give a custom `psr3_severity_mapper()` to `psr3_log_record_converter()`:

```php
<?php

use Flow\Telemetry\Logger\Severity;
use Psr\Log\LogLevel;
use function Flow\Bridge\Psr3\Telemetry\DSL\{psr3_log_record_converter, psr3_severity_mapper, psr3_telemetry_logger};

$converter = psr3_log_record_converter(
    severityMapper: psr3_severity_mapper([
        LogLevel::DEBUG     => Severity::TRACE,
        LogLevel::INFO      => Severity::INFO,
        LogLevel::NOTICE    => Severity::WARN,
        LogLevel::WARNING   => Severity::WARN,
        LogLevel::ERROR     => Severity::ERROR,
        LogLevel::CRITICAL  => Severity::FATAL,
        LogLevel::ALERT     => Severity::FATAL,
        LogLevel::EMERGENCY => Severity::FATAL,
    ]),
);

$psrLogger = psr3_telemetry_logger($telemetry->logger('my-service'), $converter);
```

## Context Handling

### Attributes

Each context entry becomes an attribute on the emitted `LogRecord`, with the same key:

```php
$psrLogger->info('Request processed', [
    'http.method' => 'GET',
    'http.status' => 200,
    'duration_ms' => 12.4,
]);
// attributes: {http.method: 'GET', http.status: 200, duration_ms: 12.4}
```

`AttributeValueNormalizer` normalizes non-scalar values:

- `null` → `'null'`
- `DateTimeInterface`, `Throwable` → passed through
- arrays → recursively normalized
- objects with `__toString()` → string cast
- objects without `__toString()` → class name
- other types → `get_debug_type()` result

### Exceptions

`LogRecord::setException()` unpacks a `Throwable` under the `exception` key into the standard OTEL exception
attributes:

```php
try {
    // ...
} catch (\Throwable $e) {
    $psrLogger->error('Operation failed', ['exception' => $e]);
}
// attributes:
//   exception.type:       'RuntimeException'
//   exception.message:    '...'
//   exception.stacktrace: '...'
```

The original `exception` key is not stored as a separate attribute. A non-`Throwable` value under `exception` is a
regular attribute.

### Message Interpolation

As PSR-3 §1.2 says, matching context entries replace the `{placeholder}` tokens in the message body. Only scalars
and `Stringable` objects are used. Arrays, `Throwable` instances, and plain objects stay in the template. The context
entries are always also available as attributes.

```php
$psrLogger->warning('Quota exceeded for {tenant} ({used}/{limit})', [
    'tenant' => 'acme',
    'used'   => 105,
    'limit'  => 100,
]);
// body:       'Quota exceeded for acme (105/100)'
// attributes: {tenant: 'acme', used: 105, limit: 100}
```

## DSL Functions

### psr3_telemetry_logger()

Wraps a Flow Telemetry `Logger` in a PSR-3 `LoggerInterface`.

```php
$psrLogger = psr3_telemetry_logger(
    logger: $telemetry->logger('my-service'),
    converter: $converter, // optional, defaults to LogRecordConverter()
);
```

### psr3_log_record_converter()

Builds the converter that turns each PSR-3 call into a `LogRecord`. Use it to add a custom severity mapper or value
normalizer.

```php ignore
$converter = psr3_log_record_converter(
    severityMapper: psr3_severity_mapper([...]),
    valueNormalizer: psr3_attribute_value_normalizer(),
);
```

### psr3_severity_mapper()

Builds a `SeverityMapper`. Give `null` (default) for the standard PSR-3 → OTEL mapping, or a full PSR-3 → Severity
array to override it.

```php
$mapper = psr3_severity_mapper([
    LogLevel::DEBUG => Severity::TRACE,
    // ... rest of PSR-3 levels
]);
```

### psr3_attribute_value_normalizer()

Builds the default `AttributeValueNormalizer`. It exists for symmetry with the other DSL helpers. Most users do not need it.

```php
$normalizer = psr3_attribute_value_normalizer();
```
