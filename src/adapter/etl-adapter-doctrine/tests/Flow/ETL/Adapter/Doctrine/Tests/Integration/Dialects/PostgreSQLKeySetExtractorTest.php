<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use DateTimeImmutable;
use Flow\Clock\FakeClock;
use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Adapter\Doctrine\Tests\Context\PlannedTable;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\Attributes\TestWith;

use function array_filter;
use function Flow\ETL\Adapter\Doctrine\from_dbal_key_set_qb;
use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_desc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\Adapter\Doctrine\to_dbal_schema_table;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function str_contains;

final class PostgreSQLKeySetExtractorTest extends IntegrationTestCase
{
    public function test_extracting_empty_table(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            $table = 'flow_key_set_extractor_test',
        ));

        $rows = data_frame()
            ->extract(from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                pagination_key_set(pagination_key_asc('id')),
            ))
            ->fetch()
            ->toArray();

        static::assertEmpty($rows);
    }

    public function test_extracting_entire_table(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
                str_schema('description', metadata: DbalMetadata::length(255)),
            ),
            $table = 'flow_key_set_extractor_with_a_very_long_table_name_test',
        ));

        for ($i = 1; $i <= 8; $i++) {
            $this->pgsqlDatabaseContext->insert($table, [
                'id' => $i,
                'name' => 'name_' . $i,
                'description' => 'description_' . $i,
            ]);
        }

        $data = df()
            ->read(from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                pagination_key_set(pagination_key_asc('flow_key_set_extractor_with_a_very_long_table_name_test.id')),
            ))
            ->fetch();

        static::assertSame(
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
            $data->toArray(),
        );
    }

    public function test_extracting_entire_table_by_multiple_keys(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                datetime_schema('created_at'),
                str_schema('name', metadata: DbalMetadata::length(255)),
                json_schema('tags'),
            ),
            $table = 'flow_key_set_extractor_test',
        ));

        $clock = new FakeClock();
        $clock->set(new DateTimeImmutable('2025-01-01 00:00:00 UTC'));

        for ($i = 1; $i <= 25; $i++) {
            $clock->modify('+1 hour');
            $this->pgsqlDatabaseContext->insert($table, [
                'id' => $i,
                'created_at' => $clock->now()->format('Y-m-d H:i:s'),
                'name' => 'name_' . $i,
                'tags' => '{"a": 1, "b": 2 }',
            ]);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                    pagination_key_set(pagination_key_asc('id'), pagination_key_desc('created_at')),
                )
                    ->withSchema(schema(
                        int_schema('id'),
                        str_schema('name'),
                        datetime_schema('created_at'),
                        map_schema('tags', type_map(type_string(), type_integer())),
                    ))
                    ->withMaximum(5)
                    ->withBatchSize(1),
            )
            ->fetch()
            ->toArray();

        // the NULL check, then 5 pages of 1
        static::assertSame(6, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        static::assertCount(5, $rows);
        static::assertSame(25, $rows[0]['id']);
    }

    public function test_extracting_entire_table_using_qb_with_maximum_set_on_extractor(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
                json_schema('tags'),
            ),
            $table = 'flow_key_set_extractor_test',
        ));

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, [
                'id' => $i,
                'name' => 'name_' . $i,
                'tags' => '{"a": 1, "b": 2 }',
            ]);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                    pagination_key_set(pagination_key_asc('id')),
                )
                    ->withSchema(schema(
                        int_schema('id'),
                        str_schema('name'),
                        map_schema('tags', type_map(type_string(), type_integer())),
                    ))
                    ->withMaximum(5)
                    ->withBatchSize(1),
            )
            ->fetch()
            ->toArray();

        // the NULL check, then 5 pages of 1
        static::assertSame(6, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        static::assertCount(5, $rows);
        static::assertSame(1, $rows[0]['id']);
    }

    public function test_extracting_entire_table_using_qb_with_maximum_set_on_extractor_with_descending_sort(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
                json_schema('tags'),
            ),
            $table = 'flow_key_set_extractor_test',
        ));

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, [
                'id' => $i,
                'name' => 'name_' . $i,
                'tags' => '{"a": 1, "b": 2 }',
            ]);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                    pagination_key_set(pagination_key_desc('id')),
                )
                    ->withSchema(schema(
                        int_schema('id'),
                        str_schema('name'),
                        map_schema('tags', type_map(type_string(), type_integer())),
                    ))
                    ->withMaximum(5)
                    ->withBatchSize(1),
            )
            ->fetch()
            ->toArray();

        // the NULL check, then 5 pages of 1
        static::assertSame(6, $this->pgsqlDatabaseContext->numberOfExecutedSelectQueries());
        static::assertCount(5, $rows);
        static::assertSame(25, $rows[0]['id']);
    }

    public function test_extracting_with_duplicate_key_values_and_tiebreaker(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                datetime_schema('created_at'),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            $table = 'flow_key_set_extractor_test',
        ));

        $createdAt = '2025-01-01 12:00:00';

        for ($i = 1; $i <= 5; $i++) {
            $this->pgsqlDatabaseContext->insert($table, [
                'id' => $i,
                'created_at' => $createdAt,
                'name' => 'name_' . $i,
            ]);
        }

        $rows = data_frame()
            ->extract(from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('*'),
                pagination_key_set(pagination_key_asc('created_at'), pagination_key_asc('id')),
            )->withBatchSize(2))
            ->fetch()
            ->toArray();

        static::assertCount(5, $rows);
        static::assertEquals(
            [
                ['id' => 1, 'created_at' => new DateTimeImmutable($createdAt), 'name' => 'name_1'],
                ['id' => 2, 'created_at' => new DateTimeImmutable($createdAt), 'name' => 'name_2'],
                ['id' => 3, 'created_at' => new DateTimeImmutable($createdAt), 'name' => 'name_3'],
                ['id' => 4, 'created_at' => new DateTimeImmutable($createdAt), 'name' => 'name_4'],
                ['id' => 5, 'created_at' => new DateTimeImmutable($createdAt), 'name' => 'name_5'],
            ],
            $rows,
        );
    }

    public function test_extraction_when_key_is_ambiguous_column(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            $table = 'flow_key_set_extractor_test_01',
        ));

        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                int_schema('id_01'),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            'flow_key_set_extractor_test_02',
        ));

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i]);

            $this->pgsqlDatabaseContext->insert('flow_key_set_extractor_test_02', [
                'id' => $i,
                'id_01' => $i,
                'name' => 'name_' . $i,
            ]);
        }

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext
                        ->connection()
                        ->createQueryBuilder()
                        ->from($table)
                        ->select('flow_key_set_extractor_test_01.id as id')
                        ->leftJoin(
                            'flow_key_set_extractor_test_01',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_01.id = flow_key_set_extractor_test_02.id_01',
                        ),
                    pagination_key_set(pagination_key_desc('flow_key_set_extractor_test_01.id')),
                )
                    ->withSchema(schema(int_schema('id')))
                    ->withBatchSize(5)
                    ->withMaximum(5),
            )
            ->fetch()
            ->toArray();

        static::assertSame(
            [
                ['id' => 25],
                ['id' => 24],
                ['id' => 23],
                ['id' => 22],
                ['id' => 21],
            ],
            $rows,
        );
    }

    public function test_extraction_when_key_is_ambiguous_column_with_custom_key_column_alias_suffix(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            $table = 'flow_key_set_extractor_test_01',
        ));

        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(
                int_schema('id', metadata: DbalMetadata::primaryKey()),
                int_schema('id_01'),
                str_schema('name', metadata: DbalMetadata::length(255)),
            ),
            'flow_key_set_extractor_test_02',
        ));

        for ($i = 1; $i <= 25; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'name' => 'name_' . $i]);

            $this->pgsqlDatabaseContext->insert('flow_key_set_extractor_test_02', [
                'id' => $i,
                'id_01' => $i,
                'name' => 'name_' . $i,
            ]);
        }

        $rows = data_frame()
            ->extract(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext
                        ->connection()
                        ->createQueryBuilder()
                        ->from($table)
                        ->select('flow_key_set_extractor_test_01.id as id')
                        ->leftJoin(
                            'flow_key_set_extractor_test_01',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_02',
                            'flow_key_set_extractor_test_01.id = flow_key_set_extractor_test_02.id_01',
                        ),
                    pagination_key_set(pagination_key_desc('flow_key_set_extractor_test_01.id')),
                )
                    ->withKeyAliasSuffix('_something_custom')
                    ->withSchema(schema(int_schema('id')))
                    ->withBatchSize(5)
                    ->withMaximum(5),
            )
            ->fetch()
            ->toArray();

        static::assertSame(
            [
                ['id' => 25],
                ['id' => 24],
                ['id' => 23],
                ['id' => 22],
                ['id' => 21],
            ],
            $rows,
        );
    }

    public function test_statistics_are_the_plan_of_the_base_query(): void
    {
        PlannedTable::create($this->pgsqlDatabaseContext, 'flow_doctrine_statistics_test', 100);

        static::assertEquals(
            Cardinality::approximately(100),
            from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext
                    ->connection()
                    ->createQueryBuilder()
                    ->select('*')
                    ->from('flow_doctrine_statistics_test'),
                pagination_key_set(pagination_key_asc('id')),
            )->statistics()->rows,
        );
    }

    public function test_null_in_secondary_key_throws_before_first_row(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(int_schema('a'), int_schema('b', true)),
            $table = 'flow_key_set_extractor_nulls_test',
        ));
        $this->pgsqlDatabaseContext->insert($table, ['a' => 1, 'b' => 1]);
        $this->pgsqlDatabaseContext->insert($table, ['a' => 1, 'b' => null]);
        $this->pgsqlDatabaseContext->insert($table, ['a' => 2, 'b' => 1]);

        $generator = from_dbal_key_set_qb(
            $this->pgsqlDatabaseContext->connection(),
            $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('a', 'b'),
            pagination_key_set(pagination_key_asc('b'), pagination_key_asc('a')),
        )
            ->withBatchSize(2)
            ->extract(flow_context());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "a", "b"',
        );

        $generator->current();
    }

    #[TestWith([2])]
    #[TestWith([3])]
    #[TestWith([5])]
    #[TestWith([6])]
    #[TestWith([10])]
    public function test_null_keys_throw_at_every_batch_size(int $batchSize): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(int_schema('id', metadata: DbalMetadata::primaryKey()), int_schema('k', true)),
            $table = 'keyset_null_repro',
        ));

        foreach ([[1, 1], [2, 2], [3, 3], [4, 4], [5, null], [6, null]] as [$id, $k]) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $id, 'k' => $k]);
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "k"',
        );

        df()
            ->read(from_dbal_key_set_qb(
                $this->pgsqlDatabaseContext->connection(),
                $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('id', 'k'),
                pagination_key_set(pagination_key_asc('k')),
            )->withBatchSize($batchSize))
            ->fetch();
    }

    public function test_short_first_page_sends_no_further_page_query(): void
    {
        $this->pgsqlDatabaseContext->createTable(to_dbal_schema_table(
            schema(int_schema('id', metadata: DbalMetadata::primaryKey()), int_schema('k')),
            $table = 'keyset_short_page',
        ));

        for ($i = 1; $i <= 6; $i++) {
            $this->pgsqlDatabaseContext->insert($table, ['id' => $i, 'k' => $i]);
        }

        $this->pgsqlDatabaseContext->resetSelectQueryCounter();

        $rows = df()
            ->read(
                from_dbal_key_set_qb(
                    $this->pgsqlDatabaseContext->connection(),
                    $this->pgsqlDatabaseContext->connection()->createQueryBuilder()->from($table)->select('id', 'k'),
                    pagination_key_set(pagination_key_asc('k')),
                )
                    ->withSchema(schema(int_schema('id'), int_schema('k')))
                    ->withBatchSize(10),
            )
            ->fetch();

        static::assertCount(6, $rows);
        static::assertCount(1, array_filter(
            $this->pgsqlDatabaseContext->executedSelectQueries(),
            static fn(string $query): bool => str_contains($query, 'ORDER BY'),
        ));
    }
}
