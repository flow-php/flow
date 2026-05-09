---
package: flow-php/phpunit-postgresql-bridge
---

# PHPUnit PostgreSQL Bridge

PHPUnit extension for flow-php/postgresql that can wrap tests in transactions.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/phpunit-postgresql-bridge.md).

## Usage

### PHPUnit Extension Configuration

Add the extension to your `phpunit.xml` or `phpunit.xml.dist`:

```xml
<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\PostgreSQL\PostgreSQLExtension"/>
</extensions>
```

### Connecting to the Database

Use the `static_pgsql_client()` DSL function instead of `pgsql_client()` in your tests:

```php
use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;

$client = static_pgsql_client(pgsql_connection_dsn($dsn));
```

When the PHPUnit extension is active, this returns a cached client wrapped in a transaction. When inactive, it delegates directly to `PgSqlClient::connect`.

For Symfony bundle integration, see the [PostgreSQL Bundle documentation](/documentation/components/bridges/symfony-postgresql-bundle.md).

### How It Works

1. When PHPUnit starts, the extension enables `StaticClient` caching
2. Before each test, the extension rolls back any previous transaction and begins a new one
3. All database operations during the test run inside this transaction
4. When the test ends, the next test's preparation rolls back all changes
5. After all tests finish, all connections are closed

This means database changes made during a test are automatically undone, keeping your test database clean.

### Nested Transactions

The flow-php/postgresql library supports nested transactions via SAVEPOINTs. If your test code calls `beginTransaction()`, it creates a savepoint (level 2+). When the extension rolls back (level 1), all nested changes are undone.

### Skipping Transaction Rollback

Use the `#[SkipTransactionRollback]` attribute to opt out of transaction wrapping for specific tests:

```php
use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;

// Skip for an entire class
#[SkipTransactionRollback]
final class MigrationTest extends TestCase
{
    // Tests in this class will NOT be wrapped in a transaction
}

// Skip for a specific method
final class SomeTest extends TestCase
{
    #[SkipTransactionRollback]
    public function test_something_that_needs_real_commits(): void
    {
        // This test will NOT be wrapped in a transaction
    }
}

// Skip via abstract parent class (applies to all subclasses)
#[SkipTransactionRollback]
abstract class AbstractMigrationTest extends TestCase
{
}

final class ConcreteMigrationTest extends AbstractMigrationTest
{
    // Inherits skip behavior from parent
}
```
