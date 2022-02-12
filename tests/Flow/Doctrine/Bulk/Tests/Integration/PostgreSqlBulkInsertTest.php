<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\Doctrine\Bulk\Tests\IntegrationTestCase;

final class PostgreSqlBulkInsertTest extends IntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        BulkInsert::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        BulkInsert::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        BulkInsert::create()->insertOrSkipOnConflict(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => false],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
            ])
        );

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
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
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );
        BulkInsert::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        BulkInsert::create()->insertOrUpdateOnConstraintConflict(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            'flow_doctrine_bulk_test_pkey',
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ])
        );

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );
        BulkInsert::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        BulkInsert::create()->insertOrUpdateOnConflict(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            ['id'],
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ])
        );

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns_with_update_only_specific_columns() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );
        BulkInsert::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        BulkInsert::create()->insertOrUpdateOnConflict(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            ['id'],
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'DESCRIPTION', 'active' => true],
            ]),
            ['description']
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'DESCRIPTION', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }
}
