<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\{from_dbal_queries, from_dbal_query};
use function Flow\ETL\DSL\{df, from_array, int_schema, map_schema, schema, str_schema, type_int, type_map, type_string};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\{DbalLoader, ParametersSet};
use Flow\ETL\Flow;

final class DbalQueryExtractorTest extends IntegrationTestCase
{
    public function test_extracting_multiple_rows_at_once() : void
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

        (new Flow())->extract(
            from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        )->load(
            DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table)
        )->run();

        $rows = df()->extract(
            from_dbal_query(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id"
            )
        )->fetch();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ],
            $rows->toArray()
        );
    }

    public function test_extracting_multiple_rows_multiple_times() : void
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

        (new Flow())
            ->extract(
                from_array([
                    ['id' => 1, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 2, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 3, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 4, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 5, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 6, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 7, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 8, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 9, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 10, 'name' => 'Name', 'description' => 'Description'],
                ])
            )
            ->load(DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table))
            ->run();

        $rows = (new Flow())->extract(
            from_dbal_queries(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                )
            )
        )->fetch();

        self::assertSame(10, $rows->count());
        self::assertSame(
            [
                ['id' => 1, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 2, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 3, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 4, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 5, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 6, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 7, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 8, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 9, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 10, 'name' => 'Name', 'description' => 'Description'],
            ],
            $rows->toArray()
        );
    }

    public function test_extracting_multiple_rows_with_schema() : void
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

        (new Flow())
            ->extract(
                from_array([
                    ['id' => 1, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 2, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 3, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 4, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 5, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 6, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 7, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 8, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 9, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                    ['id' => 10, 'name' => 'Name', 'tags' => '{"a": 1, "b": 2 }'],
                ])
            )
            ->load(DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table))
            ->run();

        $schema = (new Flow())->extract(
            from_dbal_queries(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                )
            )->withSchema(schema(
                int_schema('id'),
                str_schema('name'),
                map_schema('tags', type_map(type_string(), type_int()))
            ))
        )->schema();

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
}
