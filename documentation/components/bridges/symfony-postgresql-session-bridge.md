# Symfony PostgreSQL Session Bridge

A Symfony session handler backed by Flow PHP's native PostgreSQL library. Replaces `PdoSessionHandler` without requiring PDO or Doctrine DBAL — sessions are stored directly in PostgreSQL using Flow's query builder and client.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-session-bridge)
- [GitHub](https://github.com/flow-php/symfony-postgresql-session-bridge)
- [API Reference](/documentation/api/bridge/symfony-postgresql-session)

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-session-bridge:~--FLOW_PHP_VERSION--
```

This package is used together with the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md), which registers the session handler automatically.

## Usage

TODO: Add usage documentation
