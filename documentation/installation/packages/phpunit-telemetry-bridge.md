---
seo_title: "Installing PHPUnit Telemetry Bridge"
seo_description: >
  How to install flow-php/phpunit-telemetry-bridge in your PHP project using Composer.
---

# PHPUnit Telemetry Bridge

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/bridges/phpunit-telemetry-bridge.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/phpunit-telemetry-bridge)

[TOC]

## Composer

```bash
composer require flow-php/phpunit-telemetry-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [phpunit/phpunit](https://packagist.org/packages/phpunit/phpunit)

## Optional Dependencies

The default `curl` transport works with the core dependencies only. To use the `grpc` transport, install:

- PHP extension [ext-grpc](https://github.com/grpc/grpc/tree/master/src/php)
- [grpc/grpc](https://packagist.org/packages/grpc/grpc)
- [google/protobuf](https://packagist.org/packages/google/protobuf)
- [open-telemetry/gen-otlp-protobuf](https://packagist.org/packages/open-telemetry/gen-otlp-protobuf)

```bash
composer require --dev grpc/grpc google/protobuf open-telemetry/gen-otlp-protobuf
```
