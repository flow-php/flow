---
package: flow-php/etl-adapter-logger
seo_title: "Installing Logger Adapter"
seo_description: >
  How to install flow-php/etl-adapter-logger in your PHP project using Composer.
---

# Logger Adapter

[PACKAGE_NAV:install]

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
