<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Tests\IntegrationTestCase;

final class PostgreSqlBulkInsertTest extends IntegrationTestCase
{
    public function test_inserts_deprecated_json_array_row() : void
    {
        if (!\defined('Doctrine\DBAL\Types\Types::JSON_ARRAY')) {
            $this->markTestSkipped('DBAL version >= 3.0');
        }

        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::STRING), ['notnull' => true]),
                    new Column('age', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
                ],
            ))
                ->setPrimaryKey(['id'])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => $id1 = \uniqid(), 'age' => 20, 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id2 = \uniqid(), 'age' => 30, 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id3 = \uniqid(), 'age' => 40, 'tags' => \json_encode(['a', 'b', 'c'])],
            ])
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());

        $this->assertSame(
            [
                ['id' => $id1, 'age' => 20, 'tags' => '["a", "b", "c"]'],
                ['id' => $id2, 'age' => 30, 'tags' => '["a", "b", "c"]'],
                ['id' => $id3, 'age' => 40, 'tags' => '["a", "b", "c"]'],
            ],
            $this->pgsqlDatabaseContext->connection()->executeQuery("SELECT * FROM {$table} ORDER BY age ASC")->fetchAllAssociative()
        );
    }

    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('age', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => false]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                    new Column('updated_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
                    new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => $id1 = \uniqid(), 'age' => 20, 'name' => 'Name One', 'description' => 'Description One', 'active' => false, 'updated_at' => $date1 = new \DateTimeImmutable(), 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id2 = \uniqid(), 'age' => 30, 'name' => 'Name Two', 'description' => null, 'active' => true, 'updated_at' => $date2 = new \DateTimeImmutable(), 'tags' => \json_encode(['a', 'b', 'c'])],
                ['id' => $id3 = \uniqid(), 'age' => 40, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false, 'updated_at' => $date3 = new \DateTimeImmutable(), 'tags' => \json_encode(['a', 'b', 'c'])],
            ])
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());

        $this->assertSame(
            [
                ['id' => $id1, 'age' => 20, 'name' => 'Name One', 'description' => 'Description One', 'active' => false, 'updated_at' => $date1->format('Y-m-d H:i:s'), 'tags' => '["a", "b", "c"]'],
                ['id' => $id2, 'age' => 30, 'name' => 'Name Two', 'description' => null, 'active' => true, 'updated_at' => $date2->format('Y-m-d H:i:s'), 'tags' => '["a", "b", "c"]'],
                ['id' => $id3, 'age' => 40, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false, 'updated_at' => $date3->format('Y-m-d H:i:s'), 'tags' => '["a", "b", "c"]'],
            ],
            $this->pgsqlDatabaseContext->connection()->executeQuery("SELECT * FROM {$table} ORDER BY age ASC")->fetchAllAssociative()
        );
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

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
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
        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
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
        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'DESCRIPTION', 'active' => true],
            ]),
            [
                'conflict_columns' => ['id'],
                'update_columns' => ['description'],
            ]
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
        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ])
        );

        Bulk::create()->insert(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ]),
            [
                'constraint' => 'flow_doctrine_bulk_test_pkey',
            ]
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
}
