<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Exception;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_delete;
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\Adapter\Doctrine\to_dbal_transaction;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function getenv;

final class SqliteTransactionalDbalLoaderTest extends IntegrationTestCase
{
    public function test_multiple_batches_in_separate_transactions(): void
    {
        if (!getenv('SQLITE_DATABASE_PATH')) {
            static::markTestSkipped('SQLite database is not available');
        }

        $table = new Table('test_table', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('value', Type::getType(Types::INTEGER), ['notnull' => true]),
        ]);
        $table->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create());

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $loader = to_dbal_transaction($connection, to_dbal_table_insert($connection, 'test_table'));

        $batch1 = rows(schema(integer_schema('id'), integer_schema('value')), row(['id' => 1, 'value' => 100]));
        $batch2 = rows(schema(integer_schema('id'), integer_schema('value')), row(['id' => 2, 'value' => 200]));

        $context = flow_context(config());

        $loader->load($batch1, $context);
        $loader->load($batch2, $context);

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        static::assertCount(2, $result);
        static::assertEquals(1, $result[0]['id']);
        static::assertEquals(100, $result[0]['value']);
        static::assertEquals(2, $result[1]['id']);
        static::assertEquals(200, $result[1]['value']);
    }

    public function test_rollback_on_failure(): void
    {
        if (!getenv('SQLITE_DATABASE_PATH')) {
            static::markTestSkipped('SQLite database is not available');
        }

        $table = new Table('test_table', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]);
        $table->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create());

        $this->sqliteDatabaseContext->createTable($table);
        $this->sqliteDatabaseContext->insert('test_table', ['id' => 1, 'name' => 'Initial']);
        $this->sqliteDatabaseContext->insert('test_table', ['id' => 2, 'name' => 'Initial']);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(schema(integer_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Should fail']));

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_delete($connection, 'test_table'),
            to_dbal_table_insert($connection, 'test_table'),
        );

        try {
            $loader->load($rows, flow_context(config()));
        } catch (Exception) {
        }

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        static::assertCount(2, $result);
        static::assertEquals(1, $result[0]['id']);
        static::assertEquals('Initial', $result[0]['name']);
        static::assertEquals(2, $result[1]['id']);
        static::assertEquals('Initial', $result[1]['name']);
    }

    public function test_transactional_delete_and_insert(): void
    {
        if (!getenv('SQLITE_DATABASE_PATH')) {
            static::markTestSkipped('SQLite database is not available');
        }

        $table = new Table('test_table', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]);
        $table->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create());

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(
            schema(integer_schema('id'), string_schema('name')),
            row(['id' => 1, 'name' => 'Updated']),
            row(['id' => 2, 'name' => 'Updated']),
        );

        $loader = to_dbal_transaction(
            $connection,
            to_dbal_table_delete($connection, 'test_table'),
            to_dbal_table_insert($connection, 'test_table'),
        );

        $loader->load($rows, flow_context(config()));

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        static::assertCount(2, $result);
        static::assertEquals(1, $result[0]['id']);
        static::assertEquals('Updated', $result[0]['name']);
        static::assertEquals(2, $result[1]['id']);
        static::assertEquals('Updated', $result[1]['name']);
    }

    public function test_with_isolation_level(): void
    {
        if (!getenv('SQLITE_DATABASE_PATH')) {
            static::markTestSkipped('SQLite database is not available');
        }

        $table = new Table('test_table', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]);
        $table->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create());

        $this->sqliteDatabaseContext->createTable($table);

        $connection = $this->sqliteDatabaseContext->connection();

        $rows = rows(schema(integer_schema('id'), string_schema('name')), row(['id' => 1, 'name' => 'Test']));

        $loader = to_dbal_transaction($connection, to_dbal_table_insert(
            $connection,
            'test_table',
        ))->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);

        $loader->load($rows, flow_context(config()));

        $result = $this->sqliteDatabaseContext->selectAll('test_table');

        static::assertCount(1, $result);
        static::assertEquals(1, $result[0]['id']);
        static::assertEquals('Test', $result[0]['name']);
    }
}
