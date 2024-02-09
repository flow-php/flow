<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\Tests\PostgreSqlIntegrationTestCase;

final class PostgreSqlBulkUpdateTest extends PostgreSqlIntegrationTestCase
{
    public function test_update_multiple_rows_with_all_columns_and_multiple_primary_keys_at_once() : void
    {
        $this->databaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('account', Type::getType(Types::STRING), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                    new Column('created_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id', 'account'])
        );

        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'account' => 'Bob', 'name' => 'Name One', 'description' => 'Description One', 'active' => false, 'created_at' => new \DateTime('2021-01-01 10:00:00')],
                ['id' => 2, 'account' => 'Bob', 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true, 'created_at' => new \DateTime('2021-01-01 10:00:00')],
                ['id' => 3, 'account' => 'Joe', 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false, 'created_at' => new \DateTime('2021-01-01 10:00:00')],
            ])
        );

        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'account' => 'Bob', 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false, 'created_at' => new \DateTime('2021-01-02 10:00:00')],
                ['id' => 3, 'account' => 'Joe', 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true, 'created_at' => new \DateTime('2021-01-02 20:00:00')],
            ]),
            [
                'primary_key_columns' => [
                    'id',
                    'account',
                ],
            ]
        );

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false, 'account' => 'Bob', 'created_at' => (new \DateTimeImmutable('2021-01-01 10:00:00'))->format('Y-m-d H:i:s')],
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false, 'account' => 'Bob', 'created_at' => (new \DateTimeImmutable('2021-01-02 10:00:00'))->format('Y-m-d H:i:s')],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true, 'account' => 'Joe', 'created_at' => (new \DateTimeImmutable('2021-01-02 20:00:00'))->format('Y-m-d H:i:s')],
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_update_multiple_rows_with_all_columns_at_once() : void
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
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true],
            ]),
            [
                'primary_key_columns' => [
                    'id',
                ],
            ]
        );

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true],
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_update_multiple_rows_with_selected_columns_at_once() : void
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
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => true],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => false],
            ]),
            [
                'primary_key_columns' => [
                    'id',
                ],
                'update_columns' => [
                    'name',
                ],
            ]
        );

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Description Three', 'active' => false],
            ],
            $this->databaseContext->selectAll($table)
        );
    }

    public function test_update_when_bulk_data_has_not_all_columns_from_primary_key_columns() : void
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
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('All columns from primary_key_columns must be in bulk data columns.');

        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => true],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => false],
            ]),
            [
                'primary_key_columns' => ['not_existing_column'],
            ]
        );
    }

    public function test_update_with_empty_primary_key_columns() : void
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
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('primary_key_columns option is required for update');

        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => true],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => false],
            ])
        );
    }
}
