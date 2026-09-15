<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests;

use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;

use function extension_loaded;
use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function getenv;

/**
 * These tests fail rather than skip when PostgreSQL is absent: a skipped test would let `just test`
 * go green while proving nothing about what the extractors derive from result metadata. Deliberate
 * local exception to the suite-wide skip convention - do not "restore consistency".
 */
abstract class IntegrationTestCase extends FlowTestCase
{
    /**
     * Outside the transaction StaticClient opens for every test, so an extractor reading through it runs its own
     * transaction. Nothing rolls back what it commits: a test that writes through it cleans up after itself.
     */
    protected Client $autoCommitClient;

    protected Client $client;

    protected function setUp(): void
    {
        if (!extension_loaded('pgsql')) {
            static::fail('ext-pgsql is not available');
        }

        if (!extension_loaded('pg_query')) {
            static::fail('ext-pg_query is not available');
        }

        $dsn = getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            static::fail('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->client = static_pgsql_client(pgsql_connection_dsn($dsn));
        $this->autoCommitClient = pgsql_client(pgsql_connection_dsn($dsn));
    }

    protected function tearDown(): void
    {
        $this->autoCommitClient->close();

        parent::tearDown();
    }
}
