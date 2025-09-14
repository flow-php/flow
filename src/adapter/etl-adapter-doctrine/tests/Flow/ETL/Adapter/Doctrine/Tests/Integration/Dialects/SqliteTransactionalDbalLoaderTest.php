<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use function Flow\ETL\Adapter\Doctrine\{to_dbal_table_delete, to_dbal_table_insert, to_dbal_transaction};
use function Flow\ETL\DSL\{config, flow_context, integer_entry, row, rows, string_entry};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\{TransactionIsolationLevel};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

final class SqliteTransactionalDbalLoaderTest extends IntegrationTestCase
{
    public function test_multiple_batches_in_separate_transactions() : void
    {
        if (!\getenv('SQLITE_DATABASE_PATH')) {
            self::markTestSkipped('SQLite database is not available');
        }

        $table = new Table(
            'test_table',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('value', Type::getType(Types::INTEGER), ['notnull' => true]),
            ],
        );
        $table->setPrimaryKey(['id']);

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_insert($connection, 'test_table')
        );

        $batch1 = rows(row(integer_entry('id', 1), integer_entry('value', 100)));
        $batch2 = rows(row(integer_entry('id', 2), integer_entry('value', 200)));

        $context = flow_context(config());

        $loader->load($batch1, $context);
        $loader->load($batch2, $context);

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        self::assertCount(2, $result);
        self::assertEquals(1, $result[0]['id']);
        self::assertEquals(100, $result[0]['value']);
        self::assertEquals(2, $result[1]['id']);
        self::assertEquals(200, $result[1]['value']);
    }

    public function test_rollback_on_failure() : void
    {
        if (!\getenv('SQLITE_DATABASE_PATH')) {
            self::markTestSkipped('SQLite database is not available');
        }

        $table = new Table(
            'test_table',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        );
        $table->setPrimaryKey(['id']);

        $this->sqliteDatabaseContext->createTable($table);
        $this->sqliteDatabaseContext->insert('test_table', ['id' => 1, 'name' => 'Initial']);
        $this->sqliteDatabaseContext->insert('test_table', ['id' => 2, 'name' => 'Initial']);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(row(integer_entry('id', 1), string_entry('name', 'Should fail')));

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_delete($connection, 'test_table'),
            to_dbal_table_insert($connection, 'test_table')
        );

        try {
            $loader->load($rows, flow_context(config()));
        } catch (\Exception) {
        }

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        self::assertCount(2, $result);
        self::assertEquals(1, $result[0]['id']);
        self::assertEquals('Initial', $result[0]['name']);
        self::assertEquals(2, $result[1]['id']);
        self::assertEquals('Initial', $result[1]['name']);
    }

    public function test_transactional_delete_and_insert() : void
    {
        if (!\getenv('SQLITE_DATABASE_PATH')) {
            self::markTestSkipped('SQLite database is not available');
        }

        $table = new Table(
            'test_table',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        );
        $table->setPrimaryKey(['id']);

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(row(integer_entry('id', 1), string_entry('name', 'Updated')), row(integer_entry('id', 2), string_entry('name', 'Updated')));

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_delete($connection, 'test_table'),
            to_dbal_table_insert($connection, 'test_table')
        );

        $loader->load($rows, flow_context(config()));

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        self::assertCount(2, $result);
        self::assertEquals(1, $result[0]['id']);
        self::assertEquals('Updated', $result[0]['name']);
        self::assertEquals(2, $result[1]['id']);
        self::assertEquals('Updated', $result[1]['name']);
    }

    public function test_with_isolation_level() : void
    {
        if (!\getenv('SQLITE_DATABASE_PATH')) {
            self::markTestSkipped('SQLite database is not available');
        }

        $table = new Table(
            'test_table',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        );
        $table->setPrimaryKey(['id']);

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(row(integer_entry('id', 1), string_entry('name', 'Test')));

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_insert($connection, 'test_table')
        )->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);

        $loader->load($rows, flow_context(config()));

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        self::assertCount(1, $result);
        self::assertEquals(1, $result[0]['id']);
        self::assertEquals('Test', $result[0]['name']);
    }
}
