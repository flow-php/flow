<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

use function Flow\ETL\Adapter\Doctrine\from_dbal_queries;
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class SqliteDbalQueryExtractorTest extends IntegrationTestCase
{
    public function test_extracting_multiple_rows_at_once(): void
    {
        $this->sqliteDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()));

        data_frame()
            ->extract(from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ]))
            ->load(DbalLoader::fromConnection($this->sqliteDatabaseContext->connection(), $table))
            ->run();

        $rows = df()
            ->extract(from_dbal_query($this->sqliteDatabaseContext->connection(), "SELECT * FROM {$table} ORDER BY id"))
            ->fetch();

        static::assertSame(
            [
                ['id' => '1', 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => '2', 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => '3', 'name' => 'Name Three', 'description' => 'Description Three'],
            ],
            $rows->toArray(),
        );
    }

    public function test_extracting_multiple_rows_multiple_times(): void
    {
        $this->sqliteDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()));

        data_frame()
            ->extract(from_array([
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
            ]))
            ->load(DbalLoader::fromConnection($this->sqliteDatabaseContext->connection(), $table))
            ->run();

        $rows = data_frame()
            ->extract(from_dbal_queries(
                $this->sqliteDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                ),
            ))
            ->fetch();

        static::assertSame(10, $rows->count());
        static::assertSame(
            [
                ['id' => '1', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '2', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '3', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '4', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '5', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '6', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '7', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '8', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '9', 'name' => 'Name', 'description' => 'Description'],
                ['id' => '10', 'name' => 'Name', 'description' => 'Description'],
            ],
            $rows->toArray(),
        );
    }

    public function test_extracting_multiple_rows_with_schema(): void
    {
        $this->sqliteDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'length' => 255]),
        ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()));

        data_frame()
            ->extract(from_array([
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
            ]))
            ->load(DbalLoader::fromConnection($this->sqliteDatabaseContext->connection(), $table))
            ->run();

        $schema = data_frame()
            ->extract(from_dbal_queries(
                $this->sqliteDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                ),
            )->withSchema(schema(
                int_schema('id'),
                str_schema('name'),
                map_schema('tags', type_map(type_string(), type_integer())),
            )))
            ->schema();

        static::assertEquals(
            [
                [
                    'ref' => 'id',
                    'type' => [
                        'type' => 'integer',
                    ],
                    'nullable' => false,
                    'metadata' => [],
                ],
                [
                    'ref' => 'name',
                    'type' => [
                        'type' => 'string',
                    ],
                    'nullable' => false,
                    'metadata' => [],
                ],
                [
                    'ref' => 'tags',
                    'type' => [
                        'type' => 'map',
                        'key' => [
                            'type' => 'string',
                        ],
                        'value' => [
                            'type' => 'integer',
                        ],
                    ],
                    'nullable' => false,
                    'metadata' => [],
                ],
            ],
            $schema->normalize(),
        );
    }
}
