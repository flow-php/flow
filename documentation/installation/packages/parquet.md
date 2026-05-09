---
seo_title: "Installing Parquet"
seo_description: >
  How to install flow-php/parquet in your PHP project using Composer.
---

# Parquet

[DOC_LINK:/documentation/components/libs/parquet.md]

- [📜 Documentation](/documentation/components/libs/parquet.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/parquet)

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

These extensions are typically bundled with PHP but may need to be explicitly enabled.

## Suggested: Arrow Extension

For high-performance Parquet reading/writing via Apache Arrow, install the arrow PHP extension:

```bash
pie install flow-php/arrow-ext
```

See [Arrow Extension installation](/documentation/installation/packages/arrow-ext.md) for full details.
