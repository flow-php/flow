<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\DSL\Dbal;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

final class DbalLimitOffsetExtractorTest extends IntegrationTestCase
{
    public function test_extracting_entire_table() : void
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

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'description' => 'description_' . $i]);
        }

        $data = (new Flow())
            ->extract(
                Dbal::from_limit_offset(
                    $this->pgsqlDatabaseContext->connection(),
                    $table,
                    new OrderBy('id', Order::ASC),
                    5
                )
            )
            ->transform(Transform::array_unpack('row'))
            ->drop('row')
            ->fetch();

        $this->assertSame(
            [
                ['id' => 1, 'name' => 'name_1', 'description' => 'description_1'],
                ['id' => 2, 'name' => 'name_2', 'description' => 'description_2'],
                ['id' => 3, 'name' => 'name_3', 'description' => 'description_3'],
                ['id' => 4, 'name' => 'name_4', 'description' => 'description_4'],
                ['id' => 5, 'name' => 'name_5', 'description' => 'description_5'],
                ['id' => 6, 'name' => 'name_6', 'description' => 'description_6'],
                ['id' => 7, 'name' => 'name_7', 'description' => 'description_7'],
                ['id' => 8, 'name' => 'name_8', 'description' => 'description_8'],
            ],
            $data->toArray()
        );
    }

    public function test_extracting_limited_number_of_rows_from_table() : void
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

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'description' => 'description_' . $i]);
        }

        $data = (new Flow())
            ->extract(
                Dbal::from_limit_offset(
                    $this->pgsqlDatabaseContext->connection(),
                    $table,
                    new OrderBy('id', Order::ASC),
                    5,
                    7
                )
            )
            ->transform(Transform::array_unpack('row'))
            ->drop('row')
            ->fetch();

        $this->assertSame(
            [
                ['id' => 1, 'name' => 'name_1', 'description' => 'description_1'],
                ['id' => 2, 'name' => 'name_2', 'description' => 'description_2'],
                ['id' => 3, 'name' => 'name_3', 'description' => 'description_3'],
                ['id' => 4, 'name' => 'name_4', 'description' => 'description_4'],
                ['id' => 5, 'name' => 'name_5', 'description' => 'description_5'],
                ['id' => 6, 'name' => 'name_6', 'description' => 'description_6'],
                ['id' => 7, 'name' => 'name_7', 'description' => 'description_7'],
            ],
            $data->toArray()
        );
    }

    public function test_extracting_selected_columns() : void
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

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'description' => 'description_' . $i]);
        }

        $data = (new Flow())
            ->extract(
                Dbal::from_limit_offset(
                    $this->pgsqlDatabaseContext->connection(),
                    new \Flow\ETL\Adapter\Doctrine\Table($table, ['name']),
                    new OrderBy('id', Order::ASC),
                    5,
                    7
                )
            )
            ->transform(Transform::array_unpack('row'))
            ->drop('row')
            ->fetch();

        $this->assertSame(
            [
                ['name' => 'name_1'],
                ['name' => 'name_2'],
                ['name' => 'name_3'],
                ['name' => 'name_4'],
                ['name' => 'name_5'],
                ['name' => 'name_6'],
                ['name' => 'name_7'],
            ],
            $data->toArray()
        );
    }

    public function test_querybuilder_must_have_order_by_parts_defined(): void
    {
        $this->expectExceptionMessageMatches('/order by/');
        new DbalLimitOffsetExtractor(
            $conn = $this->pgsqlDatabaseContext->connection(),
            (new QueryBuilder($conn))->from('any')->select('*'),
        );
    }

    public function test_querybuilder_must_have_from_parts_defined(): void
    {
        $this->expectExceptionMessageMatches('/table to select from/');
        new DbalLimitOffsetExtractor(
            $conn = $this->pgsqlDatabaseContext->connection(),
            (new QueryBuilder($conn))->orderBy('any')->select('*'),
        );
    }

    public function test_querybuilder_must_have_columns_defined(): void
    {
        $this->expectExceptionMessageMatches('/at least one column/');
        new DbalLimitOffsetExtractor(
            $conn = $this->pgsqlDatabaseContext->connection(),
            (new QueryBuilder($conn))->orderBy('any')->from('table'),
        );
    }
}
