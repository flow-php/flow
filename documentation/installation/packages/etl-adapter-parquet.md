---
package: flow-php/etl-adapter-parquet
seo_title: "Installing Parquet Adapter"
seo_description: >
  How to install flow-php/etl-adapter-parquet in your PHP project using Composer.
---

# Parquet Adapter

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-parquet:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/parquet](https://packagist.org/packages/flow-php/parquet)

## Optional Extensions

- [arrow-ext](/documentation/installation/packages/arrow-ext.md) - Parquet files are decoded and encoded in Rust.
- [flow_php](/documentation/installation/packages/flow-php-ext.md) - with arrow-ext also loaded, batches pass between
  the extensions without PHP values.

The adapter conflicts with `ext-arrow <0.45` and `ext-flow_php <0.45`.
