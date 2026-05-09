---
seo_title: "Installing Symfony PostgreSQL Cache Bridge"
seo_description: >
  How to install flow-php/symfony-postgresql-cache-bridge in your PHP project using Composer.
---

# Symfony PostgreSQL Cache Bridge

[DOC_LINK:/documentation/components/bridges/symfony-postgresql-cache-bridge.md]

- [Documentation](/documentation/components/bridges/symfony-postgresql-cache-bridge.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-cache-bridge)

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
