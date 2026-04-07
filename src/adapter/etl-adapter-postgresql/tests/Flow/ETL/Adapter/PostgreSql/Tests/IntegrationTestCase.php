<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests;

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;

abstract class IntegrationTestCase extends FlowTestCase
{
    protected Client $client;

    protected function setUp() : void
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

        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    protected function tearDown() : void
    {
        if (isset($this->client)) {
            $this->client->close();
        }
    }
}
