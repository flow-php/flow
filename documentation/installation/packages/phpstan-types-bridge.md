---
package: flow-php/phpstan-types-bridge
seo_title: "Installing PHPStan Types Bridge"
seo_description: >
  How to install flow-php/phpstan-types-bridge in your PHP project using Composer.
---

# PHPStan Types Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require --dev flow-php/phpstan-types-bridge:~--FLOW_PHP_VERSION--
```

With [phpstan/extension-installer](https://github.com/phpstan/extension-installer) the extension is
registered automatically. Otherwise include it in your `phpstan.neon`:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```
