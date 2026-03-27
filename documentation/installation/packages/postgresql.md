---
seo_title: "Installing PostgreSQL Library"
seo_description: >
  How to install flow-php/postgresql in your PHP project using Composer.
---

# PostgreSQL Library

- [⬅️️ Back](/documentation/installation.md)
- [📜 Documentation](/documentation/components/libs/postgresql.md)
- [📦 Packagist](https://packagist.org/packages/flow-php/postgresql)

[TOC]

## Composer

```bash
composer require flow-php/postgresql:~--FLOW_PHP_VERSION--
```

## Core Dependencies

- [google/protobuf](https://packagist.org/packages/google/protobuf)

## Suggested Extensions

- `ext-pgsql` — required for Client, database connections and query execution (bundled with PHP, enable via your system package manager)
- `ext-protobuf` — for faster protobuf parsing performance (bundled with PHP, enable via your system package manager)

```bash
# Required for QueryBuilder/Parser - SQL parsing and AST manipulation
pie install flow-php/pg-query-ext
```
