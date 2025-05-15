<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb,
    pagination_key_asc,
    pagination_key_desc,
    pagination_key_set,
    to_dbal_schema_table};
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{datetime_schema, df, int_schema, json_schema, map_schema, schema, str_schema};
use function Flow\Types\DSL\{type_integer, type_map, type_string};
use Flow\Clock\FakeClock;
use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\InvalidArgumentException;

final class DbalKeySetExtractorTest extends IntegrationTestCase
{
    public function test_extracting_empty_table() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'))
                )
            )
            ->fetch()
            ->toArray();

        self::assertEmpty($rows);
    }

    public function test_extracting_entire_table() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                    str_schema('description', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'description' => 'description_' . $i]);
        }

        $data = df()
            ->read(from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                    ->from($table)
                    ->select('*'),
                pagination_key_set(pagination_key_asc('id')),
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

    public function test_extracting_entire_table_by_multiple_keys() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    datetime_schema('created_at'),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                    json_schema('tags'),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $clock = new FakeClock();
        $clock->set(new \DateTimeImmutable('2025-01-01 00:00:00 UTC'));

        for ($i = 1; $i <= 25; $i++) {
            $clock->modify('+1 hour');
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'created_at' => $clock->now()->format('Y-m-d H:i:s'), 'name' => 'name_' . $i, 'tags' => '{"a": 1, "b": 2 }']);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = (data_frame())
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'), pagination_key_desc('created_at')),
                )->withSchema(schema(
                    int_schema('id'),
                    str_schema('name'),
                    datetime_schema('created_at'),
                    map_schema('tags', type_map(type_string(), type_integer()))
                ))->withMaximum(5)->withPageSize(1)
            )->fetch()->toArray();

        self::assertSame(5, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        self::assertCount(5, $rows);
        self::assertSame(25, $rows[0]['id']);
    }

    public function test_extracting_entire_table_using_qb_with_maximum_set_on_extractor() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                    json_schema('tags'),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'tags' => '{"a": 1, "b": 2 }']);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = (data_frame())
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id')),
                )->withSchema(schema(
                    int_schema('id'),
                    str_schema('name'),
                    map_schema('tags', type_map(type_string(), type_integer()))
                ))->withMaximum(5)->withPageSize(1)
            )->fetch()->toArray();

        self::assertSame(5, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        self::assertCount(5, $rows);
        self::assertSame(1, $rows[0]['id']);
    }

    public function test_extracting_entire_table_using_qb_with_maximum_set_on_extractor_with_descending_sort() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                    json_schema('tags'),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i, 'tags' => '{"a": 1, "b": 2 }']);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = (data_frame())
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_desc('id')),
                )->withSchema(schema(
                    int_schema('id'),
                    str_schema('name'),
                    map_schema('tags', type_map(type_string(), type_integer()))
                ))->withMaximum(5)->withPageSize(1)
            )->fetch()->toArray();

        self::assertSame(5, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        self::assertCount(5, $rows);
        self::assertSame(25, $rows[0]['id']);
    }

    public function test_extracting_with_duplicate_key_values_and_tiebreaker() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    datetime_schema('created_at'),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $createdAt = '2025-01-01 12:00:00';

        for ($i = 1; $i <= 5; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'created_at' => $createdAt, 'name' => 'name_' . $i]);
        }

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('created_at'), pagination_key_asc('id'))
                )->withPageSize(2)
            )
            ->fetch()
            ->toArray();

        self::assertCount(5, $rows);
        self::assertSame(
            [
                ['id' => 1, 'created_at' => $createdAt, 'name' => 'name_1'],
                ['id' => 2, 'created_at' => $createdAt, 'name' => 'name_2'],
                ['id' => 3, 'created_at' => $createdAt, 'name' => 'name_3'],
                ['id' => 4, 'created_at' => $createdAt, 'name' => 'name_4'],
                ['id' => 5, 'created_at' => $createdAt, 'name' => 'name_5'],
            ],
            $rows
        );
    }

    public function test_extraction_when_key_is_ambiguous_column() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test_01',
            )
        );

        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    int_schema('id_01'),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                'flow_key_set_extractor_test_02',
            )
        );

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i]);

            $this->pgsqlDatabaseContext->insert('flow_key_set_extractor_test_02', ['id' => $i, 'id_01' => $i, 'name' => 'name_' . $i]);
        }

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('flow_key_set_extractor_test_01.id as id')
                        ->leftJoin(
                            'flow_key_set_extractor_test_01',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_01.id = flow_key_set_extractor_test_02.id_01'
                        ),
                    pagination_key_set(pagination_key_desc('flow_key_set_extractor_test_01.id'))
                )
                ->withSchema(schema(int_schema('id')))
                ->withPageSize(5)
                ->withMaximum(5)
            )
            ->fetch()
            ->toArray();

        self::assertSame([
            ['id' => 25],
            ['id' => 24],
            ['id' => 23],
            ['id' => 22],
            ['id' => 21],
        ], $rows);
    }

    public function test_extraction_when_key_is_ambiguous_column_with_custom_key_column_alias_suffix() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test_01',
            )
        );

        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    int_schema('id_01'),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                'flow_key_set_extractor_test_02',
            )
        );

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i]);

            $this->pgsqlDatabaseContext->insert('flow_key_set_extractor_test_02', ['id' => $i, 'id_01' => $i, 'name' => 'name_' . $i]);
        }

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('flow_key_set_extractor_test_01.id as id')
                        ->leftJoin(
                            'flow_key_set_extractor_test_01',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_01.id = flow_key_set_extractor_test_02.id_01'
                        ),
                    pagination_key_set(pagination_key_desc('flow_key_set_extractor_test_01.id'))
                )
                    ->withKeyAliasSuffix('_something_custom')
                    ->withSchema(schema(int_schema('id')))
                    ->withPageSize(5)
                    ->withMaximum(5)
            )
            ->fetch()
            ->toArray();

        self::assertSame([
            ['id' => 25],
            ['id' => 24],
            ['id' => 23],
            ['id' => 22],
            ['id' => 21],
        ], $rows);
    }

    public function test_throws_exception_for_empty_key_set() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('KeySet must contain at least one key for pagination');

        from_dbal_key_set_qb(
            $this->pgsqlDatabaseContext->connection(),
            $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                ->from($table)
                ->select('*'),
            pagination_key_set()
        );
    }

    public function test_throws_exception_for_invalid_maximum() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->pgsqlDatabaseContext->insert($table, ['id' => 1, 'name' => 'name_1']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -1');

        data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'))
                )->withMaximum(-1)
            )
            ->fetch();
    }

    public function test_throws_exception_for_invalid_page_size() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->pgsqlDatabaseContext->insert($table, ['id' => 1, 'name' => 'name_1']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got 0');

        data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                        ->from($table)
                        ->select('*'),
                    pagination_key_set(pagination_key_asc('id'))
                )->withPageSize(0)
            )
            ->fetch();
    }

    public function test_throws_exception_for_pre_existing_order_by_in_query_builder() : void
    {
        $this->pgsqlDatabaseContext->createTable(
            to_dbal_schema_table(
                schema(
                    int_schema('id', metadata: DbalMetadata::primaryKey()),
                    str_schema('name', metadata: DbalMetadata::length(255)),
                ),
                $table = 'flow_key_set_extractor_test',
            )
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Keyset pagination cannot be used with an ORDER BY clause, please remove OrderBy from Query Builder');

        from_dbal_key_set_qb(
            $this->pgsqlDatabaseContext->connection(),
            $this->pgsqlDatabaseContext->connection()->createQueryBuilder()
                ->from($table)
                ->select('*')
                ->orderBy('id', 'ASC'),
            pagination_key_set(pagination_key_asc('id'))
        );
    }
}
