<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{Bulk, BulkData};
use Flow\Doctrine\Bulk\Tests\PostgreSqlIntegrationTestCase;

final class PostgreSqlBulkDeleteTest extends PostgreSqlIntegrationTestCase
{
    public function test_delete_nonexistent_rows() : void
    {
        $this->databaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        $this->databaseContext->connection()->executeStatement(
            "INSERT INTO {$table} (id, name) VALUES
            (1, 'Name One'),
            (2, 'Name Two'),
            (3, 'Name Three')"
        );

        self::assertEquals(3, $this->databaseContext->tableCount($table));

        Bulk::create()->delete(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ])
        );

        self::assertEquals(3, $this->databaseContext->tableCount($table));
        self::assertEquals(1, $this->executedQueriesCount());

        $remainingRows = $this->databaseContext->selectAll($table);
        self::assertCount(3, $remainingRows);
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One'],
                ['id' => 2, 'name' => 'Name Two'],
                ['id' => 3, 'name' => 'Name Three'],
            ],
            $remainingRows
        );
    }

    public function test_delete_rows_with_composite_key_condition() : void
    {
        $this->databaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('group_name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 1]),
                    new Column('value', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))
            ->setPrimaryKey(['id', 'group_name'])
        );

        $this->databaseContext->connection()->executeStatement(
            "INSERT INTO {$table} (id, group_name, value) VALUES
            (1, 'A', 'Value One A'),
            (1, 'B', 'Value One B'),
            (2, 'A', 'Value Two A'),
            (2, 'B', 'Value Two B'),
            (3, 'A', 'Value Three A'),
            (3, 'B', 'Value Three B')"
        );

        self::assertEquals(6, $this->databaseContext->tableCount($table));

        Bulk::create()->delete(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'group_name' => 'A'],
                ['id' => 2, 'group_name' => 'B'],
                ['id' => 3, 'group_name' => 'A'],
            ])
        );

        self::assertEquals(3, $this->databaseContext->tableCount($table));
        self::assertEquals(1, $this->executedQueriesCount()); // Only one bulk DELETE query

        $remainingRows = $this->databaseContext->selectAll($table);
        self::assertCount(3, $remainingRows);
        self::assertEquals(
            [
                ['id' => 1, 'group_name' => 'B', 'value' => 'Value One B'],
                ['id' => 2, 'group_name' => 'A', 'value' => 'Value Two A'],
                ['id' => 3, 'group_name' => 'B', 'value' => 'Value Three B'],
            ],
            $remainingRows
        );
    }

    public function test_delete_rows_with_single_column_condition() : void
    {
        $this->databaseContext->createTable(
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

        $this->databaseContext->connection()->executeStatement(
            "INSERT INTO {$table} (id, name, description) VALUES
            (1, 'Name One', 'Description One'),
            (2, 'Name Two', 'Description Two'),
            (3, 'Name Three', 'Description Three'),
            (4, 'Name Four', 'Description Four'),
            (5, 'Name Five', 'Description Five')"
        );

        self::assertEquals(5, $this->databaseContext->tableCount($table));

        Bulk::create()->delete(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1],
                ['id' => 3],
                ['id' => 5],
            ])
        );

        self::assertEquals(2, $this->databaseContext->tableCount($table));
        self::assertEquals(1, $this->executedQueriesCount()); // Only one bulk DELETE query

        $remainingRows = $this->databaseContext->selectAll($table);
        self::assertCount(2, $remainingRows);
        self::assertEquals(
            [
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 4, 'name' => 'Name Four', 'description' => 'Description Four'],
            ],
            $remainingRows
        );
    }

    public function test_delete_with_custom_types_using_casted_placeholders_works_with_postgresql() : void
    {
        $this->databaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('category', Type::getType(Types::STRING), ['notnull' => true, 'length' => 100]),
                ],
            ))
            ->setPrimaryKey(['id', 'name', 'category'])
        );

        $this->databaseContext->connection()->executeStatement(
            "INSERT INTO {$table} (id, name, category) VALUES
            (1, 'Product One', 'Electronics'),
            (2, 'Product Two', 'Books'),
            (3, 'Product Three', 'Electronics'),
            (4, 'Product Four', 'Clothing')"
        );

        self::assertEquals(4, $this->databaseContext->tableCount($table));

        $customTypes = [
            'id' => Type::getType(Types::INTEGER),
            'name' => Type::getType(Types::STRING),
            'category' => Type::getType(Types::STRING),
        ];

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'Product One', 'category' => 'Electronics'],
            ['id' => 3, 'name' => 'Product Three', 'category' => 'Electronics'],
        ], $customTypes);

        Bulk::create()->delete(
            $this->databaseContext->connection(),
            $table,
            $bulkData
        );

        self::assertEquals(2, $this->databaseContext->tableCount($table));
        self::assertEquals(1, $this->executedQueriesCount());

        $remainingRows = $this->databaseContext->selectAll($table);
        self::assertCount(2, $remainingRows);
        self::assertEquals(
            [
                ['id' => 2, 'name' => 'Product Two', 'category' => 'Books'],
                ['id' => 4, 'name' => 'Product Four', 'category' => 'Clothing'],
            ],
            $remainingRows
        );
    }
}
