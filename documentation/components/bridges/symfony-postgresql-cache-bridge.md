# Symfony PostgreSQL Cache Bridge

A Symfony Cache adapter backed by Flow PHP's native PostgreSQL library. Replaces `symfony/doctrine-dbal-adapter` without requiring Doctrine DBAL — cache items are stored directly in PostgreSQL using Flow's query builder and client.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-cache-bridge)
- [GitHub](https://github.com/flow-php/symfony-postgresql-cache-bridge)
- [API Reference](/documentation/api/bridge/symfony-postgresql-cache)

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-cache-bridge:~--FLOW_PHP_VERSION--
```

This package is used together with the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md), which registers the cache adapter automatically.

## Usage

TODO: Add usage documentation
