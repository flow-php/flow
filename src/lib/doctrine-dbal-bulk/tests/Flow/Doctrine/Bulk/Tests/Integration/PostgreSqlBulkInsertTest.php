<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use DateTime;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLInsertOptions;
use Flow\Doctrine\Bulk\Tests\PostgreSqlIntegrationTestCase;

use function Flow\ETL\DSL\generate_random_string;
use function json_encode;

final class PostgreSqlBulkInsertTest extends PostgreSqlIntegrationTestCase
{
    public function test_consecutive_inserts_of_one_shape_prepare_the_statement_once(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->setPrimaryKey(['id']));

        $bulk = Bulk::create();
        $connection = $this->databaseContext->connection();

        $bulk->insert($connection, $table, new BulkData([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]));
        $preparedByFirstInsert = $this->preparedStatementsCount();

        $bulk->insert($connection, $table, new BulkData([['id' => 3, 'name' => 'c'], ['id' => 4, 'name' => 'd']]));
        static::assertSame($preparedByFirstInsert, $this->preparedStatementsCount());

        // one row is another placeholder list, so another statement
        $bulk->insert($connection, $table, new BulkData([['id' => 5, 'name' => 'e']]));
        static::assertSame($preparedByFirstInsert + 1, $this->preparedStatementsCount());

        static::assertSame(
            [
                ['id' => 1, 'name' => 'a'],
                ['id' => 2, 'name' => 'b'],
                ['id' => 3, 'name' => 'c'],
                ['id' => 4, 'name' => 'd'],
                ['id' => 5, 'name' => 'e'],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_inserts_multiple_rows_at_once(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('age', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => false]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
            new Column('updated_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => true]),
            new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
        ]))->setPrimaryKey(['id']));

        $date1 = new DateTime();

        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            [
                'id' => $id1 = generate_random_string(10),
                'age' => 20,
                'name' => 'Name One',
                'description' => 'Description One',
                'active' => false,
                'updated_at' => $date1,
                'tags' => json_encode(['a', 'b', 'c']),
            ],
            [
                'id' => $id2 = generate_random_string(10),
                'age' => 30,
                'name' => 'Name Two',
                'description' => null,
                'active' => true,
                'updated_at' => $date2 = new DateTime(),
                'tags' => json_encode(['a', 'b', 'c']),
            ],
            [
                'id' => $id3 = generate_random_string(10),
                'age' => 40,
                'name' => 'Name Three',
                'description' => 'Description Three',
                'active' => false,
                'updated_at' => $date3 = new DateTime(),
                'tags' => json_encode(['a', 'b', 'c']),
            ],
        ]));

        static::assertEquals(3, $this->databaseContext->tableCount($table));
        static::assertEquals(1, $this->executedQueriesCount());

        static::assertSame(
            [
                [
                    'id' => $id1,
                    'age' => 20,
                    'name' => 'Name One',
                    'description' => 'Description One',
                    'active' => false,
                    'updated_at' => $date1->format('Y-m-d H:i:s'),
                    'tags' => '"[\"a\",\"b\",\"c\"]"',
                ],
                [
                    'id' => $id2,
                    'age' => 30,
                    'name' => 'Name Two',
                    'description' => null,
                    'active' => true,
                    'updated_at' => $date2->format('Y-m-d H:i:s'),
                    'tags' => '"[\"a\",\"b\",\"c\"]"',
                ],
                [
                    'id' => $id3,
                    'age' => 40,
                    'name' => 'Name Three',
                    'description' => 'Description Three',
                    'active' => false,
                    'updated_at' => $date3->format('Y-m-d H:i:s'),
                    'tags' => '"[\"a\",\"b\",\"c\"]"',
                ],
            ],
            $this->databaseContext
                ->connection()
                ->executeQuery("SELECT * FROM {$table} ORDER BY age ASC")
                ->fetchAllAssociative(),
        );
    }

    public function test_inserts_new_rows_and_skip_already_existed(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));

        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
        ]));

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => false],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
            ]),
            PostgreSQLInsertOptions::fromArray([
                'skip_conflicts' => true,
            ]),
        );

        static::assertEquals(4, $this->databaseContext->tableCount($table));
        static::assertEquals(2, $this->executedQueriesCount());
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Four', 'active' => false],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));
        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
        ]));

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ]),
            PostgreSQLInsertOptions::fromArray([
                'conflict_columns' => ['id'],
            ]),
        );

        static::assertEquals(4, $this->databaseContext->tableCount($table));
        static::assertEquals(2, $this->executedQueriesCount());
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns_with_update_only_specific_columns(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));
        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
        ]));

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'DESCRIPTION', 'active' => true],
            ]),
            PostgreSQLInsertOptions::fromArray([
                'conflict_columns' => ['id'],
                'update_columns' => ['description'],
            ]),
        );

        static::assertEquals(3, $this->databaseContext->tableCount($table));
        static::assertEquals(2, $this->executedQueriesCount());
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'DESCRIPTION', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_columns_with_update_only_specific_columns_and_preserve_existing_values(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => false, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => false, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));
        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
        ]));

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => null, 'active' => true],
                ['id' => 3, 'name' => null, 'description' => 'DESCRIPTION', 'active' => true],
            ]),
            PostgreSQLInsertOptions::fromArray([
                'conflict_columns' => ['id'],
                'update_columns' => ['name', 'description'],
                'preserve_existing_values' => true,
            ]),
        );

        static::assertEquals(3, $this->databaseContext->tableCount($table));
        static::assertEquals(2, $this->executedQueriesCount());
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'Description Two', 'active' => false],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'DESCRIPTION', 'active' => true],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_primary_key(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));
        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => false],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => true],
        ]));

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ]),
            PostgreSQLInsertOptions::fromArray([
                'constraint' => 'flow_doctrine_bulk_test_pkey',
            ]),
        );

        static::assertEquals(4, $this->databaseContext->tableCount($table));
        static::assertEquals(2, $this->executedQueriesCount());
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => true],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two', 'active' => true],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three', 'active' => false],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table),
        );
    }
}
