---
seo_title: "Installing Snappy"
seo_description: >
  How to install flow-php/snappy in your PHP project using Composer.
---

# Snappy

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/libs/snappy.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/snappy)

[TOC]

## Composer

```bash
composer require flow-php/snappy:~--FLOW_PHP_VERSION--
```

> This package is a pure PHP polyfill for the Snappy compression algorithm. For significantly better performance, it is highly recommended to install the [ext-snappy](https://github.com/krakjoe/snappy) PHP extension. When `ext-snappy` is loaded, this library will automatically use the native implementation instead of the PHP polyfill.
