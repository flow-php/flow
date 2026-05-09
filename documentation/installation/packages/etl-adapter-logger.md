---
seo_title: "Installing Logger Adapter"
seo_description: >
  How to install flow-php/etl-adapter-logger in your PHP project using Composer.
---

# Logger Adapter

[DOC_LINK:/documentation/components/adapters/logger.md]

- [📜 Documentation](/documentation/components/adapters/logger.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/etl-adapter-logger)

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-logger:~--FLOW_PHP_VERSION--
```

## Logger Implementation

This adapter requires a PSR-3 compatible logger. The `psr/log` interface is installed automatically,
but you need an actual implementation, for example:

```bash
composer require monolog/monolog
```
