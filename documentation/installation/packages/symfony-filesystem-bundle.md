---
seo_title: "Installing Symfony Filesystem Bundle"
seo_description: >
  How to install flow-php/symfony-filesystem-bundle in your PHP project using Composer.
---

# Symfony Filesystem Bundle

- [Back](/documentation/installation.md)
- [Documentation](/documentation/components/bridges/symfony-filesystem-bundle.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-filesystem-bundle)

[TOC]

## Composer

```bash
composer require flow-php/symfony-filesystem-bundle:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/filesystem](/documentation/installation/packages/filesystem.md)
- [flow-php/types](/documentation/installation/packages/types.md)
- [symfony/config](https://packagist.org/packages/symfony/config)
- [symfony/console](https://packagist.org/packages/symfony/console)
- [symfony/dependency-injection](https://packagist.org/packages/symfony/dependency-injection)
- [symfony/http-kernel](https://packagist.org/packages/symfony/http-kernel)

## Optional Filesystem Bridges

Mount remote object stores by installing the matching bridge alongside the bundle:

- [flow-php/filesystem-async-aws-bridge](/documentation/installation/packages/filesystem-async-aws-bridge.md) — required for the `aws-s3` protocol
- [flow-php/filesystem-azure-bridge](/documentation/installation/packages/filesystem-azure-bridge.md) — required for the `azure-blob` protocol

## Suggested Dependencies

- [flow-php/symfony-filesystem-cache-bridge](/documentation/installation/packages/symfony-filesystem-cache-bridge.md) — for PSR-6 / Symfony Cache pools backed by any mounted filesystem (local disk, S3, Azure Blob)
- [flow-php/symfony-telemetry-bundle](/documentation/installation/packages/symfony-telemetry-bundle.md) — for telemetry integration (distributed tracing, metrics, logging)
