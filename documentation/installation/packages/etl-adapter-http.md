---
seo_title: "Installing HTTP Adapter"
seo_description: >
  How to install flow-php/etl-adapter-http in your PHP project using Composer.
---

# HTTP Adapter

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/adapters/http.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/etl-adapter-http)

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
