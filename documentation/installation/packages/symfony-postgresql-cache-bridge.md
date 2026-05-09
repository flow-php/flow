---
package: flow-php/symfony-postgresql-cache-bridge
seo_title: "Installing Symfony PostgreSQL Cache Bridge"
seo_description: >
  How to install flow-php/symfony-postgresql-cache-bridge in your PHP project using Composer.
---

# Symfony PostgreSQL Cache Bridge

[PACKAGE_NAV:install]

[TOC]

## Composer

```bash
composer require flow-php/symfony-postgresql-cache-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/postgresql](/documentation/installation/packages/postgresql.md)
- [symfony/cache](https://packagist.org/packages/symfony/cache)

## Recommended Packages

- [flow-php/symfony-postgresql-bundle](/documentation/installation/packages/symfony-postgresql-bundle.md) — registers the cache adapter and catalog provider automatically via Symfony bundle configuration

## Recommended Extensions

- `ext-pgsql` — required for database connections and query execution
