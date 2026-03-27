---
seo_title: "Installing Symfony Telemetry Bundle"
seo_description: >
  How to install flow-php/symfony-telemetry-bundle in your PHP project using Composer.
---

# Symfony Telemetry Bundle

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/bridges/symfony-telemetry-bundle.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/symfony-telemetry-bundle)

[TOC]

## Composer

```bash
composer require flow-php/symfony-telemetry-bundle:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [symfony/config](https://packagist.org/packages/symfony/config)
- [symfony/console](https://packagist.org/packages/symfony/console)
- [symfony/dependency-injection](https://packagist.org/packages/symfony/dependency-injection)
- [symfony/http-kernel](https://packagist.org/packages/symfony/http-kernel)

## Suggested Dependencies

```bash
# For Doctrine DBAL tracing
composer require doctrine/dbal

# For PSR-18 HTTP client tracing
composer require flow-php/psr18-telemetry-bridge:~--FLOW_PHP_VERSION--

# For OTLP exporter support
composer require flow-php/telemetry-otlp-bridge:~--FLOW_PHP_VERSION--

# For Messenger tracing middleware
composer require symfony/messenger

# For Twig template tracing
composer require twig/twig
```
