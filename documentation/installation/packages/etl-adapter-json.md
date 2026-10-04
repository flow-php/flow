---
package: flow-php/etl-adapter-json
seo_title: "Installing JSON Adapter"
seo_description: >
  How to install flow-php/etl-adapter-json in your PHP project using Composer.
---

# JSON Adapter

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-json:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [halaxa/json-machine](https://packagist.org/packages/halaxa/json-machine)

## Optional Extension

With the [flow_php extension](/documentation/installation/packages/flow-php-ext.md) loaded, JSON files are read and
written in Rust. The adapter conflicts with `ext-flow_php <0.45`.
