---
package: flow-php/etl-adapter-csv
seo_title: "Installing CSV Adapter"
seo_description: >
  How to install flow-php/etl-adapter-csv in your PHP project using Composer.
---

# CSV Adapter

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-csv:~--FLOW_PHP_VERSION--
```

## Optional Extension

With the [flow_php extension](/documentation/installation/packages/flow-php-ext.md) loaded, CSV files are read and
written in Rust. The adapter conflicts with `ext-flow_php <0.45`.
