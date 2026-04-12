---
seo_title: "Installing Symfony PostgreSQL Bundle"
seo_description: >
  How to install flow-php/symfony-postgresql-bundle in your PHP project using Composer.
---

# Symfony PostgreSQL Bundle

- [Back](/documentation/installation.md)
- [Documentation](/documentation/components/bridges/symfony-postgresql-bundle.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-bundle)

[TOC]

## Composer

```bash
composer require flow-php/symfony-postgresql-bundle:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/postgresql](/documentation/installation/packages/postgresql.md)
- [flow-php/filesystem](/documentation/installation/packages/filesystem.md)
- [symfony/config](https://packagist.org/packages/symfony/config)
- [symfony/console](https://packagist.org/packages/symfony/console)
- [symfony/dependency-injection](https://packagist.org/packages/symfony/dependency-injection)
- [symfony/http-kernel](https://packagist.org/packages/symfony/http-kernel)
- [twig/twig](https://packagist.org/packages/twig/twig)

## Recommended Extensions

- `ext-pgsql` — required for database connections and query execution
- [`ext-pg_query`](/documentation/components/extensions/pg-query-ext.md) — required for schema diffing (`migrations:diff`), SQL parsing and AST manipulation
- `ext-protobuf` — significantly faster protobuf deserialization for `ext-pg_query` AST parsing (without it, the slower userland `google/protobuf` package is used)

## Suggested Dependencies

- [flow-php/symfony-postgresql-messenger-bridge](/documentation/installation/packages/symfony-postgresql-messenger-bridge.md) — for Symfony Messenger PostgreSQL transport support (replaces `symfony/doctrine-messenger`)
- [flow-php/symfony-telemetry-bundle](/documentation/installation/packages/symfony-telemetry-bundle.md) — for telemetry integration (distributed tracing, metrics, logging)
