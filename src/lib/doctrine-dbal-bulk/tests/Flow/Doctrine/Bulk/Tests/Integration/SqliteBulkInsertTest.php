<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\Tests\SqliteIntegrationTestCase;
use Flow\Doctrine\Bulk\{Bulk, BulkData};

final class SqliteBulkInsertTest extends SqliteIntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->databaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('age', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => false]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                    new Column('updated_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => true]),
                    new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => $id1 = \bin2hex(\random_bytes(5)), 'age' => 20, 'name' => 'Name One', 'description' => 'Description One', 'active' => false, 'updated_at' => $date1 = new \DateTime(), 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id2 = \bin2hex(\random_bytes(5)), 'age' => 30, 'name' => 'Name Two', 'description' => null, 'active' => true, 'updated_at' => $date2 = new \DateTime(), 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id3 = \bin2hex(\random_bytes(5)), 'age' => 40, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false, 'updated_at' => $date3 = new \DateTime(), 'tags' => \json_encode(['a', 'b', 'c'])],
            ])
        );

        self::assertEquals(3, $this->databaseContext->tableCount($table));
        self::assertEquals(1, $this->executedQueriesCount());

        self::assertEquals(
            [
                ['id' => $id1, 'age' => 20, 'name' => 'Name One', 'description' => 'Description One', 'active' => 0, 'updated_at' => $date1->format('Y-m-d H:i:s'), 'tags' => '["a","b","c"]'],
                ['id' => $id2, 'age' => 30, 'name' => 'Name Two', 'description' => null, 'active' => 1, 'updated_at' => $date2->format('Y-m-d H:i:s'), 'tags' => '["a","b","c"]'],
                ['id' => $id3, 'age' => 40, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => 0, 'updated_at' => $date3->format('Y-m-d H:i:s'), 'tags' => '["a","b","c"]'],
            ],
            $this->databaseContext->connection()->executeQuery("SELECT * FROM {$table} ORDER BY age ASC")->fetchAllAssociative()
        );
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->databaseContext->createTable(
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

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => false],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
            ]),
            [
                'skip_conflicts' => true,
            ]
        );

        self::assertEquals(4, $this->databaseContext->tableCount($table));
        self::assertEquals(2, $this->executedQueriesCount());
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns() : void
    {
        $this->databaseContext->createTable(
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
        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ]),
            [
                'conflict_columns' => ['id'],
            ]
        );

        self::assertEquals(4, $this->databaseContext->tableCount($table));
        self::assertEquals(2, $this->executedQueriesCount());
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns_with_update_only_specific_columns() : void
    {
        $this->databaseContext->createTable(
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
        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'DESCRIPTION', 'active' => true],
            ]),
            [
                'conflict_columns' => ['id'],
                'update_columns' => ['description'],
            ]
        );

        self::assertEquals(3, $this->databaseContext->tableCount($table));
        self::assertEquals(2, $this->executedQueriesCount());
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'DESCRIPTION', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table)
        );
    }
}
