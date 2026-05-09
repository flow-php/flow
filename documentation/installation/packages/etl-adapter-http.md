---
package: flow-php/etl-adapter-http
seo_title: "Installing HTTP Adapter"
seo_description: >
  How to install flow-php/etl-adapter-http in your PHP project using Composer.
---

# HTTP Adapter

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-http:~--FLOW_PHP_VERSION--
```

## HTTP Client Implementation

This adapter requires a PSR-18 compatible HTTP client. The `psr/http-client` interface is installed automatically,
but you need an actual implementation, for example:

```bash
composer require symfony/http-client nyholm/psr7
```
