---
package: flow-php/mago-types-bridge
seo_title: "Installing Mago Types Bridge"
seo_description: >
  How to install flow-php/mago-types-bridge in your PHP project using Composer.
---

# Mago Types Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require --dev flow-php/mago-types-bridge:~--FLOW_PHP_VERSION--
```

Then register the worker in `mago.toml`:

```toml
[extension-hosts.flow]
command = ["php", "vendor/flow-php/mago-types-bridge/bin/worker.php"]

[analyzer]
plugins = ["flow/types"]
```

Requires `carthage-software/mago` 1.47.4 or newer.
