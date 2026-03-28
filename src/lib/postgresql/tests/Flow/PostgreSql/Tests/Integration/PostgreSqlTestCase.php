<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration;

use PHPUnit\Framework\TestCase;

abstract class PostgreSqlTestCase extends TestCase
{
    private ?PostgreSqlContext $pgsqlContext = null;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $this->pgsqlContext = new PostgreSqlContext();
    }

    protected function tearDown() : void
    {
        if ($this->pgsqlContext !== null) {
            $this->pgsqlContext->close();
            $this->pgsqlContext = null;
        }
    }

    protected function pgsqlContext() : PostgreSqlContext
    {
        if ($this->pgsqlContext === null) {
            static::fail('PostgreSqlContext not initialized. Ensure setUp() was called.');
        }

        return $this->pgsqlContext;
    }
}
