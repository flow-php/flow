---
package: flow-php/psr3-telemetry-bridge
---

# PSR-3 Telemetry Bridge

Flow PSR-3 Telemetry Bridge exposes a [PSR-3](https://www.php-fig.org/psr/psr-3/) `LoggerInterface` implementation
backed by Flow PHP Telemetry. Any framework or library that depends on `Psr\Log\LoggerInterface` can write through this
adapter and have its records emitted as Telemetry log signals.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/psr3-telemetry-bridge.md).

## Overview

This bridge wraps `Flow\Telemetry\Logger\Logger` behind a PSR-3 `LoggerInterface`. It:

- Maps PSR-3 log levels to Telemetry severities (8 → 6, customizable).
- Performs PSR-3 `{placeholder}` interpolation against the context array (scalars and `Stringable` only).
- Stores every context entry as a log record attribute under its raw key.
- Routes a `Throwable` under the `exception` key through `LogRecord::setException()`, populating
  `exception.type`, `exception.message`, and `exception.stacktrace`.
- Throws `Psr\Log\InvalidArgumentException` when `log()` is called with an unknown level, per spec.

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

The returned `TelemetryLogger` is a drop-in `Psr\Log\LoggerInterface`, so it can be passed to anything that accepts a
PSR-3 logger (Symfony, Laravel, third-party libraries).

## Severity Mapping

The default mapping collapses PSR-3's eight levels onto Telemetry's six OTEL-aligned severities:

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

Override the mapping by passing a custom `psr3_severity_mapper()` to `psr3_log_record_converter()`:

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

Every context entry becomes an attribute on the emitted `LogRecord`, keyed verbatim:

```php
$psrLogger->info('Request processed', [
    'http.method' => 'GET',
    'http.status' => 200,
    'duration_ms' => 12.4,
]);
// attributes: {http.method: 'GET', http.status: 200, duration_ms: 12.4}
```

Non-scalar values are normalized via `ValueNormalizer`:

- `null` → `'null'`
- `DateTimeInterface`, `Throwable` → passed through
- arrays → recursively normalized
- objects with `__toString()` → string cast
- objects without `__toString()` → class name
- other types → `get_debug_type()` result

### Exceptions

A `Throwable` under the `exception` key is unpacked into the standard OTEL exception attributes via
`LogRecord::setException()`:

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

The original `exception` key is **not** stored as a separate attribute. A non-`Throwable` value under `exception` is
treated as a regular attribute.

### Message Interpolation

Per PSR-3 §1.2, `{placeholder}` tokens in the message body are substituted with matching context entries. Only scalars
and `Stringable` objects participate; arrays, `Throwable` instances, and plain objects are left in the template. The
context entries remain available as attributes regardless.

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

Builds the converter that turns each PSR-3 call into a `LogRecord`. Use it to plug in a custom severity mapper or value
normalizer.

```php ignore
$converter = psr3_log_record_converter(
    severityMapper: psr3_severity_mapper([...]),
    valueNormalizer: psr3_value_normalizer(),
);
```

### psr3_severity_mapper()

Builds a `SeverityMapper`. Pass `null` (default) to use the standard PSR-3 → OTEL mapping, or a full PSR-3 → Severity
array to override.

```php
$mapper = psr3_severity_mapper([
    LogLevel::DEBUG => Severity::TRACE,
    // ... rest of PSR-3 levels
]);
```

### psr3_value_normalizer()

Builds the default `ValueNormalizer`. Provided for symmetry with the other DSL helpers; most users won't need it.

```php
$normalizer = psr3_value_normalizer();
```
