---
package: flow-php/parquet
seo_title: "Installing Parquet"
seo_description: >
  How to install flow-php/parquet in your PHP project using Composer.
---

# Parquet

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/parquet:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [packaged/thrift](https://packagist.org/packages/packaged/thrift)
- [flow-php/snappy](https://packagist.org/packages/flow-php/snappy)

## Required PHP Extensions

- `ext-bcmath`
- `ext-zlib`

PHP usually includes these extensions, but you can have to enable them explicitly.

## Suggested: Arrow Extension

For fast Parquet reads and writes through Apache Arrow, install the arrow PHP extension:

```bash
pie install flow-php/arrow-ext
```

`flow-php/parquet` conflicts with `ext-arrow <0.45`. Read
[Arrow Extension installation](/documentation/installation/packages/arrow-ext.md) for full details.
