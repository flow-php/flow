---
seo_title: "Installing Telemetry OTLP Bridge"
seo_description: >
  How to install flow-php/telemetry-otlp-bridge in your PHP project using Composer.
---

# Telemetry OTLP Bridge

[DOC_LINK:/documentation/components/bridges/telemetry-otlp-bridge.md]

- [📜 Documentation](/documentation/components/bridges/telemetry-otlp-bridge.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/telemetry-otlp-bridge)

[TOC]

## Composer

```bash
composer require flow-php/telemetry-otlp-bridge:~--FLOW_PHP_VERSION--
```

## Suggested Dependencies

```bash
# For Protobuf serializer (used by Curl+Protobuf or gRPC transports)
composer require google/protobuf
```
