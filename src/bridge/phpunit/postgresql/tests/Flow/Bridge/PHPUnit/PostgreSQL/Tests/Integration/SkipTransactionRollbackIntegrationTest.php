<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Integration;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Bridge\PHPUnit\PostgreSQL\DSL\static_pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function getenv;

/**
 * Tests run in declaration order. The sequence proves:
 * 1. Normal test data is rolled back
 * 2. #[SkipTransactionRollback] on method lets data persist
 * 3. Normal test after skip is back in a transaction
 * 4. Nested transactions are still rolled back by the extension.
 */
final class SkipTransactionRollbackIntegrationTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pgsql')) {
            self::markTestSkipped('ext-pgsql is not available');
        }

        $dsn = getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            self::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }
    }

    public function test_1_normal_test_data_is_rolled_back(): void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE _test_rollback_check (id INT PRIMARY KEY, label TEXT)');
        $client->execute("INSERT INTO _test_rollback_check (id, label) VALUES (1, 'normal')");

        static::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_rollback_check'));
    }

    public function test_2_previous_normal_test_was_rolled_back(): void
    {
        $client = $this->client();

        static::assertSame(
            0,
            $client->fetchScalarInt(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_test_rollback_check' AND table_schema = 'public'",
            ),
            'Table from normal test should not exist after rollback',
        );
    }

    #[SkipTransactionRollback]
    public function test_3_skip_on_method_data_persists(): void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE _test_skip_method (id INT PRIMARY KEY, label TEXT)');
        $client->execute("INSERT INTO _test_skip_method (id, label) VALUES (1, 'persisted')");

        static::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_method'));
    }

    #[SkipTransactionRollback]
    public function test_4_skip_on_method_data_survived_and_cleanup(): void
    {
        $client = $this->client();

        static::assertSame(
            1,
            $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_method'),
            'Data from #[SkipTransactionRollback] method should persist',
        );

        $client->execute('DROP TABLE IF EXISTS _test_skip_method');
    }

    public function test_5_re_enabled_after_skip_data_is_rolled_back(): void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE _test_reenable (id INT PRIMARY KEY)');
        $client->execute('INSERT INTO _test_reenable (id) VALUES (1)');

        static::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_reenable'));
    }

    public function test_6_re_enabled_after_skip_was_rolled_back(): void
    {
        $client = $this->client();

        static::assertSame(
            0,
            $client->fetchScalarInt(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_test_reenable' AND table_schema = 'public'",
            ),
            'Transaction rollback should be re-enabled after a skipped test',
        );
    }

    public function test_7_nested_transactions_are_rolled_back(): void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE _test_nested (id INT PRIMARY KEY, label TEXT)');

        $client->beginTransaction();
        $client->execute("INSERT INTO _test_nested (id, label) VALUES (1, 'nested')");
        $client->commit();

        $client->beginTransaction();
        $client->execute("INSERT INTO _test_nested (id, label) VALUES (2, 'nested2')");
        $client->rollBack();

        static::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_nested'));
    }

    public function test_8_nested_transactions_from_previous_test_were_rolled_back(): void
    {
        $client = $this->client();

        static::assertSame(
            0,
            $client->fetchScalarInt(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_test_nested' AND table_schema = 'public'",
            ),
            'Nested transaction data should be rolled back by the extension',
        );
    }

    protected function client(): Client
    {
        return static_pgsql_client(pgsql_connection_dsn((string) getenv('PGSQL_DATABASE_URL')));
    }
}
