---
seo_title: "Installing Symfony Telemetry Bundle"
seo_description: >
  How to install flow-php/symfony-telemetry-bundle in your PHP project using Composer.
---

# Symfony Telemetry Bundle

[DOC_LINK:/documentation/components/bridges/symfony-telemetry-bundle.md]

- [📜 Documentation](/documentation/components/bridges/symfony-telemetry-bundle.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/symfony-telemetry-bundle)

[TOC]

## Composer

```bash
composer require flow-php/symfony-telemetry-bundle:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/telemetry](/documentation/installation/packages/telemetry.md)
- [flow-php/symfony-http-foundation-telemetry-bridge](/documentation/installation/packages/symfony-http-foundation-telemetry-bridge.md)
- [symfony/config](https://packagist.org/packages/symfony/config)
- [symfony/console](https://packagist.org/packages/symfony/console)
- [symfony/dependency-injection](https://packagist.org/packages/symfony/dependency-injection)
- [symfony/http-kernel](https://packagist.org/packages/symfony/http-kernel)

## Suggested Dependencies

- [flow-php/symfony-postgresql-bundle](/documentation/installation/packages/symfony-postgresql-bundle.md) — for PostgreSQL database management and migrations with telemetry support
- [flow-php/psr18-telemetry-bridge](/documentation/installation/packages/psr18-telemetry-bridge.md) — for PSR-18 HTTP client tracing
- [flow-php/telemetry-otlp-bridge](/documentation/installation/packages/telemetry-otlp-bridge.md) — for OTLP exporter support
- [symfony/messenger](https://packagist.org/packages/symfony/messenger) — for Messenger tracing middleware
- [twig/twig](https://packagist.org/packages/twig/twig) — for Twig template tracing

## Optional Dependencies

- [doctrine/dbal](https://packagist.org/packages/doctrine/dbal) — for Doctrine DBAL tracing
