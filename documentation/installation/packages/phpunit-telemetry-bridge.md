---
package: flow-php/phpunit-telemetry-bridge
seo_title: "Installing PHPUnit Telemetry Bridge"
seo_description: >
  How to install flow-php/phpunit-telemetry-bridge in your PHP project using Composer.
---

# PHPUnit Telemetry Bridge

[PACKAGE_NAV:install]

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

```bash
composer require --dev grpc/grpc google/protobuf
```
