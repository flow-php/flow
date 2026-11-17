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

- [flow_php](/documentation/installation/packages/flow-php-ext.md): Parquet files are read and written in Rust, and the
  memory limit of a run counts their bytes. No other extension is needed.
- [arrow-ext](/documentation/installation/packages/arrow-ext.md): for `flow-php/parquet` used directly, or as an explicit
  `engine: new RustParquetEngine()`. A memory limit does not count its bytes.

The adapter conflicts with `ext-arrow <0.45` and `ext-flow_php <0.45`.
