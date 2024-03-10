<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\{to_dbal_table_insert, to_dbal_table_update};
use function Flow\ETL\DSL\from_array;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Flow;

final class DbalLoaderTest extends IntegrationTestCase
{
    public function test_create_loader_with_invalid_operation() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert or update, delete given.');

        new DbalLoader($table, $this->connectionParams(), [], 'delete');
    }

    public function test_create_loader_with_invalid_operation_from_connection() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert or update, delete given.');

        DbalLoader::fromConnection(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            [],
            'delete'
        );
    }

    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $loader = to_dbal_table_insert($this->connectionParams(), $table);

        (new Flow())
            ->read(
                from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
            )
            ->load($loader)
            ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_multiple_rows_at_once_using_existing_connection() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))
                ->setPrimaryKey(['id'])
        );

        $loader = to_dbal_table_insert($this->pgsqlDatabaseContext->connection(), $table);

        (new Flow())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load($loader)
            ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
    }

    public function test_inserts_multiple_rows_in_two_insert_queries() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (new Flow())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->connectionParams(), $table))
            ->run();

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));
        (new Flow())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->connectionParams(), $table))
            ->run();

        (new Flow())
            ->read(
                from_array([
                    ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                    ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                    ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->connectionParams(), $table, ['skip_conflicts' => true]))
            ->run();

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_primary_key() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        (new Flow())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->connectionParams(), $table))
            ->run();

        (new Flow())->extract(
            from_array([
                    ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                    ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                    ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
                ])
        )
            ->load(to_dbal_table_insert($this->connectionParams(), $table, ['constraint' => 'flow_doctrine_bulk_test_pkey']))
            ->run();

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_update_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $insertLoader = to_dbal_table_insert($this->connectionParams(), $table);
        $updateLoader = to_dbal_table_update($this->connectionParams(), $table, ['primary_key_columns' => ['id'], ['update_columns' => ['name']]]);

        (new Flow())->extract(
            from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        )
        ->load($insertLoader)
        ->run();

        (new Flow())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Changed Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Changed Name Three', 'description' => 'Description Three'],
            ])
            )
        ->load($updateLoader)
        ->run();

        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Changed Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Changed Name Three', 'description' => 'Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll('flow_doctrine_bulk_test')
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
    }
}
