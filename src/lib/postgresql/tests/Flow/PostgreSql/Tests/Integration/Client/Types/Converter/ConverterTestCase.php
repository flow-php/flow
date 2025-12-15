<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn, pgsql_mapper};
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\TestCase;

abstract class ConverterTestCase extends TestCase
{
    protected Client $client;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            static::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->client = pgsql_client(
            pgsql_connection_dsn($dsn),
            mapper: pgsql_mapper(),
        );
    }

    protected function tearDown() : void
    {
        if (isset($this->client)) {
            $this->client->close();
        }
    }

    /**
     * @param array<int, mixed> $params
     *
     * @return mixed
     */
    protected function fetchValue(string $sql, array $params = [])
    {
        $row = $this->client->fetchOne($sql, $params);

        return \array_values($row)[0];
    }
}
