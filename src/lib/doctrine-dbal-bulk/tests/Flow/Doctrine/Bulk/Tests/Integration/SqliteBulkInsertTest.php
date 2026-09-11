<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use DateTime;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Dialect\SqliteInsertOptions;
use Flow\Doctrine\Bulk\Tests\Mother\WideTableMother;
use Flow\Doctrine\Bulk\Tests\SqliteIntegrationTestCase;

use function Flow\ETL\DSL\generate_random_string;

final class SqliteBulkInsertTest extends SqliteIntegrationTestCase
{
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
        ]))->setPrimaryKey(['id']));

        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            [
                'id' => $id1 = generate_random_string(10),
                'age' => 20,
                'name' => 'Name One',
                'description' => 'Description One',
                'active' => false,
                'updated_at' => $date1 = new DateTime(),
            ],
            [
                'id' => $id2 = generate_random_string(10),
                'age' => 30,
                'name' => 'Name Two',
                'description' => null,
                'active' => true,
                'updated_at' => $date2 = new DateTime(),
            ],
            [
                'id' => $id3 = generate_random_string(10),
                'age' => 40,
                'name' => 'Name Three',
                'description' => 'Description Three',
                'active' => false,
                'updated_at' => $date3 = new DateTime(),
            ],
        ]));

        static::assertEquals(3, $this->databaseContext->tableCount($table));
        static::assertEquals(1, $this->executedQueriesCount());

        static::assertEquals(
            [
                [
                    'id' => $id1,
                    'age' => 20,
                    'name' => 'Name One',
                    'description' => 'Description One',
                    'active' => 0,
                    'updated_at' => $date1->format('Y-m-d H:i:s'),
                ],
                [
                    'id' => $id2,
                    'age' => 30,
                    'name' => 'Name Two',
                    'description' => null,
                    'active' => 1,
                    'updated_at' => $date2->format('Y-m-d H:i:s'),
                ],
                [
                    'id' => $id3,
                    'age' => 40,
                    'name' => 'Name Three',
                    'description' => 'Description Three',
                    'active' => 0,
                    'updated_at' => $date3->format('Y-m-d H:i:s'),
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
            SqliteInsertOptions::fromArray([
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
            SqliteInsertOptions::fromArray([
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
            SqliteInsertOptions::fromArray([
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
            SqliteInsertOptions::fromArray([
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

    public function test_a_failing_chunk_rolls_back_the_whole_write(): void
    {
        $this->databaseContext->createTable(WideTableMother::table($table = 'flow_doctrine_bulk_rollback_test', 40));

        $rows = WideTableMother::rows(40, 2000);

        // 819 rows fit under the cap at 40 columns, so row 1000 sits in the second of three statements
        $rows[999]['c1'] = null;

        $failure = null;

        try {
            Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData($rows));
        } catch (Exception $e) {
            $failure = $e;
        }

        static::assertInstanceOf(Exception::class, $failure);
        static::assertSame(0, $this->databaseContext->tableCount($table));
    }

    public function test_insert_of_more_rows_than_the_bind_cap_succeeds(): void
    {
        $this->databaseContext->createTable(WideTableMother::table($table = 'flow_doctrine_bulk_wide_test', 40));

        // 1000 rows x 40 columns = 40 000 parameters, past SQLite's 32 766
        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData(WideTableMother::rows(40, 1000)),
        );

        static::assertSame(1000, $this->databaseContext->tableCount($table));
    }
}
