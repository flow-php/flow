<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\{from_dbal_limit_offset, from_dbal_limit_offset_qb};
use function Flow\ETL\DSL\{df, int_schema, map_schema, schema, str_schema, type_int, type_map, type_string};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\{Order, OrderBy};
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

        $data = df()
            ->read(from_dbal_limit_offset(
                $this->pgsqlDatabaseContext->connection(),
                $table,
                new OrderBy('id', Order::ASC),
                5
            ))
            ->fetch();

        self::assertSame(
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

    public function test_extracting_entire_table_using_qb() : void
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
                from_dbal_limit_offset_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*')
                        ->orderBy('id', 'ASC'),
                    5
                )
            )
            ->fetch();

        self::assertSame(
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

    public function test_extracting_entire_table_using_qb_with_schema() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'tags' => '{"a": 1, "b": 2 }']);
        }

        $schema = (new Flow())
            ->extract(
                from_dbal_limit_offset_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*')
                        ->orderBy('id', 'ASC'),
                    5
                )->withSchema(schema(
                    int_schema('id'),
                    str_schema('name'),
                    map_schema('tags', type_map(type_string(), type_int()))
                ))
            )
            ->schema();

        self::assertEquals(
            [
                [
                    'ref' => 'id',
                    'type' => [
                        'type' => 'integer',
                        'nullable' => false,
                    ],
                    'metadata' => [],
                ],
                [
                    'ref' => 'name',
                    'type' => [
                        'type' => 'string',
                        'nullable' => false,
                    ],
                    'metadata' => [],
                ],
                [
                    'ref' => 'tags',
                    'type' => [
                        'type' => 'map',
                        'key' => [
                            'type' => 'string',
                            'nullable' => false,
                        ],
                        'value' => [
                            'type' => 'integer',
                            'nullable' => false,
                        ],
                        'nullable' => false,
                    ],
                    'metadata' => [],
                ],
            ],
            $schema->normalize()
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

        $data = df()
            ->read(from_dbal_limit_offset(
                $this->pgsqlDatabaseContext->connection(),
                $table,
                new OrderBy('id', Order::ASC),
                5,
                7
            ))
            ->fetch();

        self::assertSame(
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

        $data = df()
            ->read(from_dbal_limit_offset(
                $this->pgsqlDatabaseContext->connection(),
                new \Flow\ETL\Adapter\Doctrine\Table($table, ['name']),
                new OrderBy('id', Order::ASC),
                5,
                7
            ))
            ->fetch();

        self::assertSame(
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
}
