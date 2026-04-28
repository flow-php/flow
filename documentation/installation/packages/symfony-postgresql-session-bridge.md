---
seo_title: "Installing Symfony PostgreSQL Session Bridge"
seo_description: >
  How to install flow-php/symfony-postgresql-session-bridge in your PHP project using Composer.
---

# Symfony PostgreSQL Session Bridge

- [Back](/documentation/installation.md)
- [Documentation](/documentation/components/bridges/symfony-postgresql-session-bridge.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-session-bridge)

[TOC]

## Composer

```bash
composer require flow-php/symfony-postgresql-session-bridge:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/postgresql](/documentation/installation/packages/postgresql.md)
- [symfony/http-foundation](https://packagist.org/packages/symfony/http-foundation)

## Recommended Packages

- [flow-php/symfony-postgresql-bundle](/documentation/installation/packages/symfony-postgresql-bundle.md) — registers the session handler, catalog provider, and purge command automatically via Symfony bundle configuration

## Recommended Extensions

- `ext-pgsql` — required for database connections and query execution
