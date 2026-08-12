---
package: flow-php/symfony-filesystem-cache-bridge
seo_title: "Installing Symfony Filesystem Cache Bridge"
seo_description: >
  How to install flow-php/symfony-filesystem-cache-bridge in your PHP project using Composer.
---

# Symfony Filesystem Cache Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/symfony-filesystem-cache-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/filesystem](/documentation/installation/packages/filesystem.md)
- [symfony/cache](https://packagist.org/packages/symfony/cache)

## Recommended Packages

- [flow-php/symfony-filesystem-bundle](/documentation/installation/packages/symfony-filesystem-bundle.md) - registers the cache adapter from `flow_filesystem.cache.pools` config
- [flow-php/filesystem-async-aws-bridge](/documentation/installation/packages/filesystem-async-aws-bridge.md) - to back cache pools with AWS S3
- [flow-php/filesystem-azure-bridge](/documentation/installation/packages/filesystem-azure-bridge.md) - to back cache pools with Azure Blob Storage
