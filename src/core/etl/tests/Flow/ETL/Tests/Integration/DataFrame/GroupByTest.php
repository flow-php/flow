<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\window;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_uuid;

final class GroupByTest extends FlowIntegrationTestCase
{
    public function test_a_bare_column_groups_like_a_one_element_array(): void
    {
        $dataset = [
            ['country' => 'PL', 'score' => 10],
            ['country' => 'US', 'score' => 20],
            ['country' => 'PL', 'score' => 30],
        ];

        static::assertSame(
            df()->read(from_array($dataset))->groupBy(['country'])->aggregate(sum('score'))->fetch()->toArray(),
            df()->read(from_array($dataset))->groupBy('country')->aggregate(sum('score'))->fetch()->toArray(),
        );
    }

    public function test_a_bare_reference_groups_like_a_one_element_array(): void
    {
        $dataset = [
            ['country' => 'PL', 'score' => 10],
            ['country' => 'US', 'score' => 20],
            ['country' => 'PL', 'score' => 30],
        ];

        static::assertSame(
            df()
                ->read(from_array($dataset))
                ->groupBy([ref('country')])
                ->aggregate(sum('score'))
                ->fetch()
                ->toArray(),
            df()->read(from_array($dataset))->groupBy(ref('country'))->aggregate(sum('score'))->fetch()->toArray(),
        );
    }

    public function test_group_by_array(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), int_schema('score'), json_schema('array')),
                row(['id' => 1, 'score' => 20, 'array' => type_json()->cast(['a', 'b', 'c', 'd'])]),
                row(['id' => 2, 'score' => 20, 'array' => type_json()->cast(['a', 'b', 'c', 'd'])]),
                row(['id' => 3, 'score' => 25, 'array' => type_json()->cast(['a', 'b', 'c'])]),
                row(['id' => 4, 'score' => 30, 'array' => type_json()->cast(['a', 'b', 'c'])]),
                row(['id' => 5, 'score' => 40, 'array' => type_json()->cast(['a', 'b'])]),
                row(['id' => 6, 'score' => 40, 'array' => type_json()->cast(['a', 'b'])]),
                row(['id' => 7, 'score' => 45, 'array' => type_json()->cast(['a', 'b'])]),
                row(['id' => 9, 'score' => 50, 'array' => type_json()->cast(['a'])]),
            )))
            ->groupBy(['array'])
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        static::assertEquals(
            schema(json_schema('array'), float_schema('score_sum', true), float_schema('score_avg', true)),
            $rows->schema(),
        );
        static::assertEquals(
            [
                ['array' => ['a', 'b', 'c', 'd'], 'score_sum' => 40, 'score_avg' => 20.0],
                ['array' => ['a', 'b', 'c'], 'score_sum' => 55, 'score_avg' => 27.5],
                ['array' => ['a', 'b'], 'score_sum' => 125, 'score_avg' => 41.67],
                ['array' => ['a'], 'score_sum' => 50, 'score_avg' => 50.0],
            ],
            $rows->toArray(),
        );
    }

    public function test_group_by_date_time(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), int_schema('score'), datetime_schema('date')),
                row(['id' => 1, 'score' => 20, 'date' => type_datetime()->cast('2024-01-01 10:00:00')]),
                row(['id' => 2, 'score' => 20, 'date' => type_datetime()->cast('2024-01-01 10:00:00')]),
                row(['id' => 3, 'score' => 25, 'date' => type_datetime()->cast('2024-01-02 10:00:00')]),
                row(['id' => 4, 'score' => 30, 'date' => type_datetime()->cast('2024-01-02 10:00:00')]),
                row(['id' => 5, 'score' => 40, 'date' => type_datetime()->cast('2024-01-03 10:00:00')]),
                row(['id' => 6, 'score' => 40, 'date' => type_datetime()->cast('2024-01-03 10:00:00')]),
                row(['id' => 7, 'score' => 45, 'date' => type_datetime()->cast('2024-01-03 10:00:00')]),
                row(['id' => 9, 'score' => 50, 'date' => type_datetime()->cast('2024-01-04 10:00:00')]),
            )))
            ->groupBy(['date'])
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        static::assertEquals(
            schema(datetime_schema('date'), float_schema('score_sum', true), float_schema('score_avg', true)),
            $rows->schema(),
        );
        /** @var list<array{date: DateTimeImmutable, score_sum: int, score_avg: float}> $aggregated */
        $aggregated = $rows->toArray();
        // group output order follows hash-bucket order, not input order - assert order-insensitively
        usort($aggregated, static fn(array $a, array $b): int => $a['date'] <=> $b['date']);

        static::assertEquals(
            [
                ['date' => new DateTimeImmutable('2024-01-01 10:00:00'), 'score_sum' => 40, 'score_avg' => 20.0],
                ['date' => new DateTimeImmutable('2024-01-02 10:00:00'), 'score_sum' => 55, 'score_avg' => 27.5],
                ['date' => new DateTimeImmutable('2024-01-03 10:00:00'), 'score_sum' => 125, 'score_avg' => 41.67],
                ['date' => new DateTimeImmutable('2024-01-04 10:00:00'), 'score_sum' => 50, 'score_avg' => 50.0],
            ],
            $aggregated,
        );
    }

    public function test_group_by_multiple_columns_and_batch_size(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(4))->method('load');

        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age'), str_schema('gender')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 2, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 3, 'country' => 'PL', 'age' => 25, 'gender' => 'male']),
                row(['id' => 4, 'country' => 'PL', 'age' => 30, 'gender' => 'female']),
                row(['id' => 5, 'country' => 'US', 'age' => 40, 'gender' => 'female']),
                row(['id' => 6, 'country' => 'US', 'age' => 40, 'gender' => 'male']),
                row(['id' => 7, 'country' => 'US', 'age' => 45, 'gender' => 'female']),
                row(['id' => 9, 'country' => 'US', 'age' => 50, 'gender' => 'male']),
            )))
            ->groupBy(['country', 'gender'])
            ->aggregate(average(ref('age')))
            ->withEntry('age_avg', ref('age_avg')->round(lit(2)))
            ->batchSize(1)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['country' => 'PL', 'gender' => 'male', 'age_avg' => 21.67],
                ['country' => 'PL', 'gender' => 'female', 'age_avg' => 30.0],
                ['country' => 'US', 'gender' => 'female', 'age_avg' => 42.5],
                ['country' => 'US', 'gender' => 'male', 'age_avg' => 45],
            ],
            $rows->toArray(),
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age'), str_schema('gender')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 2, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 3, 'country' => 'PL', 'age' => 25, 'gender' => 'male']),
                row(['id' => 4, 'country' => 'PL', 'age' => 30, 'gender' => 'female']),
                row(['id' => 5, 'country' => 'US', 'age' => 40, 'gender' => 'female']),
                row(['id' => 6, 'country' => 'US', 'age' => 40, 'gender' => 'male']),
                row(['id' => 7, 'country' => 'US', 'age' => 45, 'gender' => 'female']),
                row(['id' => 9, 'country' => 'US', 'age' => 50, 'gender' => 'male']),
            )))
            ->groupBy(['country', 'gender'])
            ->aggregate(average(ref('age')))
            ->fetch();

        static::assertSame(
            [
                ['country' => 'PL', 'gender' => 'male', 'age_avg' => 21.67],
                ['country' => 'PL', 'gender' => 'female', 'age_avg' => 30.0],
                ['country' => 'US', 'gender' => 'female', 'age_avg' => 42.5],
                ['country' => 'US', 'gender' => 'male', 'age_avg' => 45.0],
            ],
            $rows->toArray(),
        );
        static::assertEquals(
            schema(str_schema('country'), str_schema('gender'), float_schema('age_avg', true)),
            $rows->schema(),
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation_with_null(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(
                    int_schema('id'),
                    str_schema('country'),
                    int_schema('age'),
                    str_schema('gender', nullable: true),
                ),
                row(['id' => 1, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 2, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 3, 'country' => 'PL', 'age' => 25, 'gender' => 'male']),
                row(['id' => 4, 'country' => 'PL', 'age' => 30, 'gender' => 'female']),
                row(['id' => 5, 'country' => 'US', 'age' => 40, 'gender' => 'female']),
                row(['id' => 6, 'country' => 'US', 'age' => 40, 'gender' => 'male']),
                row(['id' => 7, 'country' => 'US', 'age' => 45, 'gender' => null]),
                row(['id' => 9, 'country' => 'US', 'age' => 50, 'gender' => 'male']),
            )))
            ->groupBy(['country', 'gender'])
            ->aggregate(average(ref('age')))
            ->fetch();

        static::assertSame(
            [
                ['country' => 'PL', 'gender' => 'female', 'age_avg' => 30.0],
                ['country' => 'US', 'gender' => 'female', 'age_avg' => 40.0],
                ['country' => 'PL', 'gender' => 'male', 'age_avg' => 21.67],
                ['country' => 'US', 'gender' => 'male', 'age_avg' => 45.0],
                ['country' => 'US', 'gender' => null, 'age_avg' => 45.0],
            ],
            $rows->sortBy(ref('gender'), ref('country'))->toArray(),
        );
        static::assertEquals(
            schema(str_schema('country'), str_schema('gender', true), float_schema('age_avg', true)),
            $rows->schema(),
        );
    }

    public function test_group_by_single_column(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            )))
            ->groupBy(['country'])
            ->aggregate(sum(ref('age')))
            ->fetch();

        static::assertEquals(
            [
                ['country' => 'PL', 'age_sum' => 95],
                ['country' => 'US', 'age_sum' => 175],
            ],
            $rows->toArray(),
        );
    }

    public function test_group_by_single_column_with_an_alias(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            )))
            ->groupBy(['country'])
            ->aggregate(sum(ref('age')->as('total_age')))
            ->fetch();

        static::assertSame(
            [
                ['country' => 'PL', 'total_age' => 95.0],
                ['country' => 'US', 'total_age' => 175.0],
            ],
            $rows->toArray(),
        );
    }

    public function test_group_by_single_column_with_avg_aggregation(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            )))
            ->groupBy(['country'])
            ->aggregate(average(ref('age')))
            ->fetch();

        static::assertSame(
            [
                ['country' => 'PL', 'age_avg' => 23.75],
                ['country' => 'US', 'age_avg' => 43.75],
            ],
            $rows->toArray(),
        );
        static::assertEquals(schema(str_schema('country'), float_schema('age_avg', true)), $rows->schema());
    }

    public function test_group_by_twice(): void
    {
        $dataset = [
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-01', 'user' => 'user_02'],
            ['date' => '2023-01-01', 'user' => 'user_03'],
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-02', 'user' => 'user_01'],
            ['date' => '2023-01-02', 'user' => 'user_02'],
            ['date' => '2023-01-02', 'user' => 'user_03'],
            ['date' => '2023-01-02', 'user' => 'user_03'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
        ];

        $rows = df()
            ->read(from_array($dataset))
            ->groupBy([ref('date'), ref('user')])
            ->aggregate(count(ref('user')))
            ->rename('user_count', 'contributions')
            ->drop('date')
            ->groupBy([ref('user')])
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        static::assertSame(
            [
                ['user' => 'user_01', 'contributions_sum' => 4.0],
                ['user' => 'user_02', 'contributions_sum' => 2.0],
                ['user' => 'user_03', 'contributions_sum' => 3.0],
                ['user' => 'user_04', 'contributions_sum' => 3.0],
            ],
            $rows->toArray(),
        );
    }

    public function test_group_by_uuid(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), int_schema('score'), uuid_schema('uuid')),
                row(['id' => 1, 'score' => 20, 'uuid' => type_uuid()->cast('b97a23ab-ba84-4d8f-9d9a-abd32cc58110')]),
                row(['id' => 2, 'score' => 20, 'uuid' => type_uuid()->cast('b97a23ab-ba84-4d8f-9d9a-abd32cc58110')]),
                row(['id' => 3, 'score' => 25, 'uuid' => type_uuid()->cast('28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae')]),
                row(['id' => 4, 'score' => 30, 'uuid' => type_uuid()->cast('28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae')]),
                row(['id' => 5, 'score' => 40, 'uuid' => type_uuid()->cast('5085fabf-15f7-4467-9076-61547afbbdc9')]),
                row(['id' => 6, 'score' => 40, 'uuid' => type_uuid()->cast('5085fabf-15f7-4467-9076-61547afbbdc9')]),
                row(['id' => 7, 'score' => 45, 'uuid' => type_uuid()->cast('5085fabf-15f7-4467-9076-61547afbbdc9')]),
                row(['id' => 9, 'score' => 50, 'uuid' => type_uuid()->cast('c7c22b40-45ad-46d1-a47b-0d1dd389ae41')]),
            )))
            ->groupBy(['uuid'])
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        static::assertEquals(
            schema(uuid_schema('uuid'), float_schema('score_sum', true), float_schema('score_avg', true)),
            $rows->schema(),
        );
        static::assertEquals(
            [
                [
                    'uuid' => Uuid::fromString('b97a23ab-ba84-4d8f-9d9a-abd32cc58110'),
                    'score_sum' => 40,
                    'score_avg' => 20.0,
                ],
                [
                    'uuid' => Uuid::fromString('28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae'),
                    'score_sum' => 55,
                    'score_avg' => 27.5,
                ],
                [
                    'uuid' => Uuid::fromString('5085fabf-15f7-4467-9076-61547afbbdc9'),
                    'score_sum' => 125,
                    'score_avg' => 41.67,
                ],
                [
                    'uuid' => Uuid::fromString('c7c22b40-45ad-46d1-a47b-0d1dd389ae41'),
                    'score_sum' => 50,
                    'score_avg' => 50.0,
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_pivot(): void
    {
        $dataset1 = [
            ['date' => '2023-11-01', 'user' => 'norberttech', 'contributions' => 5],
            ['date' => '2023-11-01', 'user' => 'stloyd', 'contributions' => 4],
            ['date' => '2023-11-02', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-02', 'user' => 'stloyd', 'contributions' => 6],
        ];

        $dataset2 = [
            ['date' => '2023-11-03', 'user' => 'norberttech', 'contributions' => 2],
            ['date' => '2023-11-03', 'user' => 'stloyd', 'contributions' => 7],
            ['date' => '2023-11-04', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-04', 'user' => 'stloyd', 'contributions' => 5],
            ['date' => '2023-11-05', 'user' => 'norberttech', 'contributions' => 7],
            ['date' => '2023-11-05', 'user' => 'stloyd', 'contributions' => 11],
        ];

        $rows = df()
            ->read(from_all(from_array($dataset1), from_array($dataset2)))
            ->groupBy([ref('date')])
            ->pivot(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        static::assertSame(
            [
                [
                    'date' => '2023-11-01',
                    'norberttech' => 5.0,
                    'stloyd' => 4.0,
                ],
                [
                    'date' => '2023-11-02',
                    'norberttech' => 3.0,
                    'stloyd' => 6.0,
                ],
                [
                    'date' => '2023-11-03',
                    'norberttech' => 2.0,
                    'stloyd' => 7.0,
                ],
                [
                    'date' => '2023-11-04',
                    'norberttech' => 3.0,
                    'stloyd' => 5.0,
                ],
                [
                    'date' => '2023-11-05',
                    'norberttech' => 7.0,
                    'stloyd' => 11.0,
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_pivot_with_multiple_group_by_column(): void
    {
        $dataset1 = [
            ['date' => '2023-11-01', 'type' => 'admin', 'user' => 'norberttech', 'contributions' => 5],
            ['date' => '2023-11-01', 'type' => 'contributor', 'user' => 'stloyd', 'contributions' => 4],
            ['date' => '2023-11-02', 'type' => 'admin', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-02', 'type' => 'contributor', 'user' => 'stloyd', 'contributions' => 6],
        ];

        $dataset2 = [
            ['date' => '2023-11-03', 'type' => 'admin', 'user' => 'norberttech', 'contributions' => 2],
            ['date' => '2023-11-03', 'type' => 'contributor', 'user' => 'stloyd', 'contributions' => 7],
            ['date' => '2023-11-04', 'type' => 'admin', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-04', 'type' => 'contributor', 'user' => 'stloyd', 'contributions' => 5],
            ['date' => '2023-11-05', 'type' => 'admin', 'user' => 'norberttech', 'contributions' => 7],
            ['date' => '2023-11-05', 'type' => 'contributor', 'user' => 'stloyd', 'contributions' => 11],
        ];

        $rows = df()
            ->read(from_all(from_array($dataset1), from_array($dataset2)))
            ->groupBy([ref('date'), ref('type')])
            ->pivot(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        static::assertSame(
            [
                [
                    'date' => '2023-11-01',
                    'type' => 'admin',
                    'norberttech' => 5.0,
                    'stloyd' => null,
                ],
                [
                    'date' => '2023-11-01',
                    'type' => 'contributor',
                    'stloyd' => 4.0,
                    'norberttech' => null,
                ],
                [
                    'date' => '2023-11-02',
                    'type' => 'admin',
                    'norberttech' => 3.0,
                    'stloyd' => null,
                ],
                [
                    'date' => '2023-11-02',
                    'type' => 'contributor',
                    'stloyd' => 6.0,
                    'norberttech' => null,
                ],
                [
                    'date' => '2023-11-03',
                    'type' => 'admin',
                    'norberttech' => 2.0,
                    'stloyd' => null,
                ],
                [
                    'date' => '2023-11-03',
                    'type' => 'contributor',
                    'stloyd' => 7.0,
                    'norberttech' => null,
                ],
                [
                    'date' => '2023-11-04',
                    'type' => 'admin',
                    'norberttech' => 3.0,
                    'stloyd' => null,
                ],
                [
                    'date' => '2023-11-04',
                    'type' => 'contributor',
                    'stloyd' => 5.0,
                    'norberttech' => null,
                ],
                [
                    'date' => '2023-11-05',
                    'type' => 'admin',
                    'norberttech' => 7.0,
                    'stloyd' => null,
                ],
                [
                    'date' => '2023-11-05',
                    'type' => 'contributor',
                    'stloyd' => 11.0,
                    'norberttech' => null,
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_standalone_avg_aggregation(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            )))
            ->aggregate([average(ref('age'))])
            ->rename('age_avg', 'average_age')
            ->fetch();

        static::assertEquals(
            rows(schema(float_schema('average_age', nullable: true)), row(['average_age' => 33.75])),
            $rows,
        );
    }

    public function test_standalone_avg_and_max_aggregation(): void
    {
        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            )))
            ->aggregate([average(ref('age')), max(ref('age'))])
            ->run(function (Rows $rows): void {
                $this->assertSame([['age_avg' => 33.75, 'age_max' => 50]], $rows->toArray());
                $this->assertEquals(
                    schema(float_schema('age_avg', true), int_schema('age_max', true)),
                    $rows->schema(),
                );
            });
    }

    /**
     * A group with a whole-number minimum and a group with a fractional one used to yield
     * IntegerEntry and FloatEntry side by side in one Rows - the declared type now comes from the
     * bind, once for the whole run. Scope: groups spilled into separate buckets still bind against
     * their own bucket's schema, so cross-bucket variance for a column typed differently per group
     * remains until Rows carries one stream-wide schema.
     */
    public function test_the_output_definition_does_not_vary_between_groups(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(str_schema('group'), float_schema('value')),
                row(['group' => 'a', 'value' => 10.0]),
                row(['group' => 'a', 'value' => 20.0]),
                row(['group' => 'b', 'value' => 0.5]),
            )))
            ->groupBy(['group'])
            ->aggregate(min(ref('value')))
            ->fetch();

        static::assertEquals(schema(str_schema('group'), float_schema('value_min', true)), $rows->schema());
        static::assertSame(
            [
                ['group' => 'a', 'value_min' => 10.0],
                ['group' => 'b', 'value_min' => 0.5],
            ],
            $rows->toArray(),
        );
    }

    public function test_window_avg_function(): void
    {
        $memoryPage1 = new ArrayMemory([
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'Michael', 'department' => 'Sales', 'salary' => 4600],
            ['employee_name' => 'Jeff', 'department' => 'Marketing', 'salary' => 3000],
            ['employee_name' => 'Saif', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'John', 'department' => 'Marketing', 'salary' => 3200],
        ]);
        $memoryPage2 = new ArrayMemory([
            ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800],
            ['employee_name' => 'Oliver', 'department' => 'Sales', 'salary' => 2900],
            ['employee_name' => 'Mia', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400],
            ['employee_name' => 'Ava', 'department' => 'Finance', 'salary' => 3800],
            ['employee_name' => 'Isabella', 'department' => 'Marketing', 'salary' => 2100],
            ['employee_name' => 'Ethan', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Charlotte', 'department' => 'Marketing', 'salary' => 3000],
        ]);

        static::assertSame(
            [
                ['department' => 'Sales', 'avg_salary' => 3917.0],
                ['department' => 'Marketing', 'avg_salary' => 2940.0],
                ['department' => 'Finance', 'avg_salary' => 3550.0],
            ],
            df()
                ->from(from_all(from_memory($memoryPage1), from_memory($memoryPage2)))
                ->withEntry('avg_salary', average(ref('salary'))->over(window()->partitionBy(ref('department'))))
                ->select('department', 'avg_salary')
                ->dropDuplicates(ref('department'), ref('avg_salary'))
                ->withEntry('avg_salary', ref('avg_salary')->round(lit(0)))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_window_rank_function(): void
    {
        $memoryPage1 = new ArrayMemory([
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'Michael', 'department' => 'Sales', 'salary' => 4600],
            ['employee_name' => 'Robert', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Maria', 'department' => 'Finance', 'salary' => 3000],
            ['employee_name' => 'Scott', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Jen', 'department' => 'Finance', 'salary' => 3900],
            ['employee_name' => 'Jeff', 'department' => 'Marketing', 'salary' => 3000],
            ['employee_name' => 'Kumar', 'department' => 'Marketing', 'salary' => 2000],
            ['employee_name' => 'Saif', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'John', 'department' => 'Marketing', 'salary' => 3200],
        ]);
        $memoryPage2 = new ArrayMemory([
            ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800],
            ['employee_name' => 'Sophia', 'department' => 'Finance', 'salary' => 4200],
            ['employee_name' => 'Oliver', 'department' => 'Sales', 'salary' => 2900],
            ['employee_name' => 'Mia', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400],
            ['employee_name' => 'Ava', 'department' => 'Finance', 'salary' => 3800],
            ['employee_name' => 'Liam', 'department' => 'Sales', 'salary' => 3100],
            ['employee_name' => 'Isabella', 'department' => 'Marketing', 'salary' => 2100],
            ['employee_name' => 'Ethan', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Charlotte', 'department' => 'Marketing', 'salary' => 3000],
        ]);

        static::assertSame(
            [
                ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800, 'rank' => 1],
                ['employee_name' => 'Sophia', 'department' => 'Finance', 'salary' => 4200, 'rank' => 1],
                ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400, 'rank' => 1],
            ],
            df()
                ->from(from_all(from_memory($memoryPage1), from_memory($memoryPage2)))
                ->dropDuplicates(ref('employee_name'), ref('department'))
                ->withEntry(
                    'rank',
                    rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())),
                )
                ->filter(ref('rank')->equals(lit(1)))
                ->fetch()
                ->toArray(),
        );
    }
}
