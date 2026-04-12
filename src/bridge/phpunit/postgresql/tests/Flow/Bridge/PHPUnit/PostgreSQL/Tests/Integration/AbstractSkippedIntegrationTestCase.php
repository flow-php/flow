<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Integration;

use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\TestCase;

#[SkipTransactionRollback]
abstract class AbstractSkippedIntegrationTestCase extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            static::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }
    }

    protected function client() : Client
    {
        return static_pgsql_client(pgsql_connection_dsn((string) \getenv('PGSQL_DATABASE_URL')));
    }
}
