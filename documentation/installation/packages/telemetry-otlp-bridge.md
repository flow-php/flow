---
seo_title: "Installing Telemetry OTLP Bridge"
seo_description: >
  How to install flow-php/telemetry-otlp-bridge in your PHP project using Composer.
---

# Telemetry OTLP Bridge

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/bridges/telemetry-otlp-bridge.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/telemetry-otlp-bridge)

[TOC]

## Composer

```bash
composer require flow-php/telemetry-otlp-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [psr/http-client](https://packagist.org/packages/psr/http-client)
- [psr/http-factory](https://packagist.org/packages/psr/http-factory)

## Suggested Dependencies

```bash
# For gRPC transport support (requires ext-grpc PHP extension)
composer require google/protobuf open-telemetry/gen-otlp-protobuf
```
