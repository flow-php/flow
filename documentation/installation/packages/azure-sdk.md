---
seo_title: "Installing Azure SDK"
seo_description: >
  How to install flow-php/azure-sdk in your PHP project using Composer.
---

# Azure SDK

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/libs/azure-sdk.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/azure-sdk)

[TOC]

## Composer

```bash
composer require flow-php/azure-sdk:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [psr/http-client](https://packagist.org/packages/psr/http-client)
- [php-http/discovery](https://packagist.org/packages/php-http/discovery)

## HTTP Client Implementation

This library requires a PSR-18 compatible HTTP client. The `psr/http-client` interface and `php-http/discovery`
are installed automatically, but you need an actual implementation, for example:

```bash
composer require symfony/http-client nyholm/psr7
```
