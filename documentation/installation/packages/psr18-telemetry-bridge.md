---
seo_title: "Installing PSR-18 Telemetry Bridge"
seo_description: >
  How to install flow-php/psr18-telemetry-bridge in your PHP project using Composer.
---

# PSR-18 Telemetry Bridge

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/bridges/psr18-telemetry-bridge.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/psr18-telemetry-bridge)

[TOC]

## Composer

```bash
composer require flow-php/psr18-telemetry-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [psr/http-client](https://packagist.org/packages/psr/http-client)

## HTTP Client Implementation

This bridge requires a PSR-18 compatible HTTP client. The `psr/http-client` interface is installed automatically,
but you need an actual implementation, for example:

```bash
composer require symfony/http-client nyholm/psr7
```
