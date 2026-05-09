---
package: flow-php/symfony-postgresql-messenger-bridge
seo_title: "Installing Symfony PostgreSQL Messenger Bridge"
seo_description: >
  How to install flow-php/symfony-postgresql-messenger-bridge in your PHP project using Composer.
---

# Symfony PostgreSQL Messenger Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/symfony-postgresql-messenger-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/postgresql](/documentation/installation/packages/postgresql.md)
- [symfony/messenger](https://packagist.org/packages/symfony/messenger)

## Recommended Packages

- [flow-php/symfony-postgresql-bundle](/documentation/installation/packages/symfony-postgresql-bundle.md) — registers the transport factory and catalog provider automatically via Symfony bundle configuration

## Recommended Extensions

- `ext-pgsql` — required for database connections and query execution
