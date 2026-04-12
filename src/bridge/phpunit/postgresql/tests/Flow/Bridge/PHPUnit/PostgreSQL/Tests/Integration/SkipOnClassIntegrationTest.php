<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Integration;

use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\TestCase;

#[SkipTransactionRollback]
final class SkipOnClassIntegrationTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            self::markTestSkipped('ext-pgsql is not available');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            self::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }
    }

    public function test_1_class_level_skip_data_persists() : void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE IF NOT EXISTS _test_skip_class (id INT PRIMARY KEY, label TEXT)');
        $client->execute("INSERT INTO _test_skip_class (id, label) VALUES (1, 'class-skip')");

        self::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_class'));
    }

    public function test_2_class_level_skip_data_survived() : void
    {
        $client = $this->client();

        self::assertSame(
            1,
            $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_class'),
            'Data should persist because #[SkipTransactionRollback] is on the class',
        );

        $client->execute('DROP TABLE IF EXISTS _test_skip_class');
    }

    protected function client() : Client
    {
        return static_pgsql_client(pgsql_connection_dsn((string) \getenv('PGSQL_DATABASE_URL')));
    }
}
