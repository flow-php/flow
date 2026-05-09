---
seo_title: "Installing PostgreSQL Adapter"
seo_description: >
  How to install flow-php/etl-adapter-postgresql in your PHP project using Composer.
---

# PostgreSQL Adapter

[DOC_LINK:/documentation/components/adapters/postgresql.md]

- [📜 Documentation](/documentation/components/adapters/postgresql.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/etl-adapter-postgresql)

[TOC]

## Composer

```bash
composer require flow-php/etl-adapter-postgresql:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [flow-php/postgresql](https://packagist.org/packages/flow-php/postgresql)

## Suggested Extensions

- `ext-pgsql` — required for database connections (bundled with PHP, enable via your system package manager)

```bash
# For SQL parsing and query building
pie install flow-php/pg-query-ext
```
