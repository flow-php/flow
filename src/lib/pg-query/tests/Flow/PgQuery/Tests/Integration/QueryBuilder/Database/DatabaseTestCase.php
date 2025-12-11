<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{drop_index, drop_materialized_view, drop_sequence, drop_table, drop_view};
use PgSql\{Connection, Result};
use PHPUnit\Framework\TestCase;

abstract class DatabaseTestCase extends TestCase
{
    protected ?Connection $connection = null;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            static::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->connection = $this->connect($dsn);
    }

    protected function tearDown() : void
    {
        if ($this->connection !== null) {
            \pg_close($this->connection);
            $this->connection = null;
        }
    }

    protected function affectedRows(Result $result) : int
    {
        return \pg_affected_rows($result);
    }

    protected function connect(string $dsn) : Connection
    {
        $parts = \parse_url($dsn);

        if ($parts === false) {
            static::fail('Failed to parse PGSQL_DATABASE_URL');
        }

        $connString = \sprintf(
            "host='%s' port='%s' dbname='%s' user='%s' password='%s'",
            $parts['host'] ?? 'localhost',
            $parts['port'] ?? '5432',
            \ltrim($parts['path'] ?? '/postgres', '/'),
            $parts['user'] ?? 'postgres',
            $parts['pass'] ?? ''
        );

        $conn = \pg_connect($connString);

        if ($conn === false) {
            static::fail('Failed to connect to PostgreSQL');
        }

        return $conn;
    }

    protected function dropIndexIfExists(string $index) : void
    {
        $this->execute(drop_index($index)->ifExists()->cascade()->toSql());
    }

    protected function dropMaterializedViewIfExists(string $view) : void
    {
        $this->execute(drop_materialized_view($view)->ifExists()->cascade()->toSql());
    }

    protected function dropSequenceIfExists(string $sequence) : void
    {
        $this->execute(drop_sequence($sequence)->ifExists()->cascade()->toSql());
    }

    protected function dropTableIfExists(string $table) : void
    {
        $this->execute(drop_table($table)->ifExists()->cascade()->toSql());
    }

    protected function dropViewIfExists(string $view) : void
    {
        $this->execute(drop_view($view)->ifExists()->cascade()->toSql());
    }

    protected function execute(string $sql) : Result|false
    {
        if ($this->connection === null) {
            static::fail('No database connection');
        }

        return \pg_query($this->connection, $sql);
    }

    /**
     * Fetch all rows from a result. Asserts result is valid.
     *
     * @return array<int, array<string, null|string>>
     */
    protected function fetchAll(Result|false $result) : array
    {
        static::assertNotFalse($result, 'Query failed');

        return \pg_fetch_all($result) ?: [];
    }

    /**
     * Fetch one row from a result. Asserts result is valid.
     *
     * @return array<string, null|string>
     */
    protected function fetchOne(Result|false $result) : array
    {
        static::assertNotFalse($result, 'Query failed');

        $row = \pg_fetch_assoc($result);
        static::assertIsArray($row, 'No row returned');

        /** @var array<string, null|string> $row */
        return $row;
    }
}
