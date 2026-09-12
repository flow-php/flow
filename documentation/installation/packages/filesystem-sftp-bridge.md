---
package: flow-php/filesystem-sftp-bridge
seo_title: "Installing Filesystem SFTP Bridge"
seo_description: >
  How to install flow-php/filesystem-sftp-bridge in your PHP project using Composer.
---

# Filesystem SFTP Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/filesystem-sftp-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [phpseclib/phpseclib](https://packagist.org/packages/phpseclib/phpseclib)

SFTP is a subsystem of SSH-2, unrelated to FTP. PHP has no built-in client for it, so this bridge
builds on phpseclib, a pure PHP implementation that needs no compiled extensions.
