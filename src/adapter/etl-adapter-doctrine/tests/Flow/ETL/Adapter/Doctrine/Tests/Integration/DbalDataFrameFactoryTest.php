<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\dbal_dataframe_factory;
use function Flow\ETL\DSL\{int_entry,
    int_schema,
    map_schema,
    ref,
    row,
    rows,
    schema,
    str_schema,
    type_int,
    type_map,
    type_string};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\{LiteralParameter, Parameter};

final class DbalDataFrameFactoryTest extends IntegrationTestCase
{
    public function test_dataframe_factory() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            'flow_doctrine_data_factory_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
        ->setPrimaryKey(['id']));

        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 1, 'name' => 'Name 1', 'description' => 'Some Description 1']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 2, 'name' => 'Name 2', 'description' => 'Some Description 2']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 3, 'name' => 'Name 3', 'description' => 'Some Description 3']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 4, 'name' => 'Name 4', 'description' => 'Some Description 4']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 5, 'name' => 'Name 5', 'description' => 'Some Description 5']);

        $rows = (
            dbal_dataframe_factory(
                $this->connectionParams(),
                'SELECT * FROM flow_doctrine_data_factory_test WHERE id IN (:ids) AND name = :name',
                Parameter::ints('ids', ref('id')),
                new LiteralParameter('name', 'Name 1')
            )
        )
        ->from(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 55)),
        ))
        ->select('id')
        ->fetch();

        self::assertSame(
            [
                ['id' => 1],
            ],
            $rows->toArray()
        );
    }

    public function test_dataframe_factory_with_schema() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            'flow_doctrine_data_factory_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 1, 'name' => 'Name 1', 'tags' => '{"a": 1, "b": 2 }']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 2, 'name' => 'Name 2', 'tags' => '{"a": 1, "b": 2 }']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 3, 'name' => 'Name 3', 'tags' => '{"a": 1, "b": 2 }']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 4, 'name' => 'Name 4', 'tags' => '{"a": 1, "b": 2 }']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 5, 'name' => 'Name 5', 'tags' => '{"a": 1, "b": 2 }']);

        $schema = (
            dbal_dataframe_factory(
                $this->connectionParams(),
                'SELECT * FROM flow_doctrine_data_factory_test WHERE id IN (:ids) AND name = :name',
                Parameter::ints('ids', ref('id')),
                new LiteralParameter('name', 'Name 1')
            )->withSchema(schema(
                int_schema('id'),
                str_schema('name'),
                map_schema('tags', type_map(type_string(), type_int()))
            ))
        )
            ->from(rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 55)),
            ))
            ->schema();

        self::assertSame(
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
}
