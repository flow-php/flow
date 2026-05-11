<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests;

use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;

use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;

abstract class IntegrationTestCase extends FlowTestCase
{
    protected Client $client;

    protected function setUp(): void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        if (!\extension_loaded('pg_query')) {
            static::markTestSkipped('ext-pg_query is not available');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            static::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->client = static_pgsql_client(pgsql_connection_dsn($dsn));
    }
}
