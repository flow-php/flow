<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Loader;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingSink;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function usort;

final class JoinTest extends FlowIntegrationTestCase
{
    public function test_join_inner(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'US'],
                    ['id' => 3, 'country' => 'FR'],
                    ['id' => 4, 'country' => 'UK'],
                    ['id' => 5, 'country' => 'GB'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'FR', 'name' => 'France'],
                        ['code' => 'CN', 'name' => 'Canada'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                )),
                join_on(['country' => 'code'], 'joined_'),
                Join::inner,
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'joined_name' => 'Poland', 'country' => 'PL', 'joined_code' => 'PL'],
                ['id' => 2, 'joined_name' => 'United States', 'country' => 'US', 'joined_code' => 'US'],
                ['id' => 3, 'joined_name' => 'France', 'country' => 'FR', 'joined_code' => 'FR'],
            ],
            $rows->toArray(),
        );
    }

    public function test_a_limit_after_a_resident_join_closes_the_loaders_before_it(): void
    {
        $sink = new RecordingSink();

        $rows = df()
            ->read((new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(10)))->withBatchSize(1))
            ->write($sink)
            ->join(
                df()->read(from_rows(RowsMother::sequentialIds(10))),
                join_on(['id' => 'id'], 'r_'),
                Join::left,
                hash_join()->storage(new MemoryBuckets()),
            )
            ->limit(2)
            ->fetch();

        static::assertCount(2, $rows);
        static::assertSame(1, $sink->closed);
        static::assertSame(0, $sink->discarded);
    }

    public function test_join_inner_with_on_disk_buckets_cache(): void
    {
        $rows = df(config_builder()->join(hash_join()->bucketsCount(4)))
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'US'],
                    ['id' => 3, 'country' => 'FR'],
                    ['id' => 4, 'country' => 'UK'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'FR', 'name' => 'France'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                )),
                join_on(['country' => 'code'], 'joined_'),
                Join::inner,
            )
            ->fetch();

        $joined = $rows->toArray();
        usort($joined, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 3, 'country' => 'FR', 'joined_code' => 'FR', 'joined_name' => 'France'],
            ],
            $joined,
        );
    }

    public function test_join_inner_with_on_disk_buckets_preserves_a_nullable_column_present_and_null(): void
    {
        $rows = df(config_builder()->join(hash_join()->bucketsCount(2)))
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL', 'note' => 'has-note'],
                    ['id' => 2, 'country' => 'US', 'note' => null],
                    ['id' => 3, 'country' => 'FR', 'note' => 'third'],
                    ['id' => 4, 'country' => 'PL', 'note' => null],
                ],
                schema(int_schema('id'), str_schema('country'), str_schema('note', nullable: true)),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'FR', 'name' => 'France'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                )),
                join_on(['country' => 'code'], 'joined_'),
                Join::inner,
            )
            ->fetch();

        $joined = $rows->toArray();
        usort($joined, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'note' => 'has-note', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'note' => null, 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 3, 'country' => 'FR', 'note' => 'third', 'joined_code' => 'FR', 'joined_name' => 'France'],
                ['id' => 4, 'country' => 'PL', 'note' => null, 'joined_code' => 'PL', 'joined_name' => 'Poland'],
            ],
            $joined,
        );
    }

    public function test_join_left(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'PL'],
                    ['id' => 3, 'country' => 'PL'],
                    ['id' => 4, 'country' => 'PL'],
                    ['id' => 5, 'country' => 'US'],
                    ['id' => 6, 'country' => 'US'],
                    ['id' => 7, 'country' => 'US'],
                    ['id' => 9, 'country' => 'US'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [['code' => 'PL', 'name' => 'Poland'], ['code' => 'US', 'name' => 'United States']],
                    schema(str_schema('code'), str_schema('name')),
                )),
                Expression::on(['country' => 'code'], 'joined_'),
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 3, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 4, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 5, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 6, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 7, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 9, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_left_anti(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(1))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'US'],
                    ['id' => 3, 'country' => 'FR'],
                    ['id' => 5, 'country' => 'GB'],
                    ['id' => 7, 'country' => 'CN'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'FR', 'name' => 'France'],
                        ['code' => 'CA', 'name' => 'Canada'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                )),
                Expression::on(['country' => 'code'], 'joined_'),
                Join::left_anti,
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 5, 'country' => 'GB'],
                ['id' => 7, 'country' => 'CN'],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_left_on_date_time_entry(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'date' => new DateTimeImmutable('2024-01-01 00:00:00')],
                    ['id' => 2, 'date' => new DateTimeImmutable('2024-01-01 00:00:00')],
                    ['id' => 3, 'date' => new DateTimeImmutable('2024-01-02 00:00:00')],
                    ['id' => 4, 'date' => new DateTimeImmutable('2024-01-03 00:00:00')],
                    ['id' => 5, 'date' => new DateTimeImmutable('2024-01-04 00:00:00')],
                    ['id' => 6, 'date' => new DateTimeImmutable('2024-01-04 00:00:00')],
                    ['id' => 7, 'date' => new DateTimeImmutable('2024-01-05 00:00:00')],
                    ['id' => 9, 'date' => new DateTimeImmutable('2024-01-05 00:00:00')],
                ],
                schema(int_schema('id'), datetime_schema('date')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['date' => new DateTimeImmutable('2024-01-01 00:00:00'), 'events' => 1],
                        ['date' => new DateTimeImmutable('2024-01-05 00:00:00'), 'events' => 5],
                    ],
                    schema(datetime_schema('date'), int_schema('events')),
                )),
                Expression::on(['date' => 'date'], 'joined_'),
                Join::left,
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'date' => new DateTimeImmutable('2024-01-01 00:00:00'),
                    'joined_date' => new DateTimeImmutable('2024-01-01 00:00:00'),
                    'joined_events' => 1,
                ],
                [
                    'id' => 2,
                    'date' => new DateTimeImmutable('2024-01-01 00:00:00'),
                    'joined_date' => new DateTimeImmutable('2024-01-01 00:00:00'),
                    'joined_events' => 1,
                ],
                [
                    'id' => 3,
                    'date' => new DateTimeImmutable('2024-01-02 00:00:00'),
                    'joined_date' => null,
                    'joined_events' => null,
                ],
                [
                    'id' => 4,
                    'date' => new DateTimeImmutable('2024-01-03 00:00:00'),
                    'joined_date' => null,
                    'joined_events' => null,
                ],
                [
                    'id' => 5,
                    'date' => new DateTimeImmutable('2024-01-04 00:00:00'),
                    'joined_date' => null,
                    'joined_events' => null,
                ],
                [
                    'id' => 6,
                    'date' => new DateTimeImmutable('2024-01-04 00:00:00'),
                    'joined_date' => null,
                    'joined_events' => null,
                ],
                [
                    'id' => 7,
                    'date' => new DateTimeImmutable('2024-01-05 00:00:00'),
                    'joined_date' => new DateTimeImmutable('2024-01-05 00:00:00'),
                    'joined_events' => 5,
                ],
                [
                    'id' => 9,
                    'date' => new DateTimeImmutable('2024-01-05 00:00:00'),
                    'joined_date' => new DateTimeImmutable('2024-01-05 00:00:00'),
                    'joined_events' => 5,
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_left_with_empty_prefix(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country_code' => 'PL'],
                    ['id' => 2, 'country_code' => 'PL'],
                    ['id' => 3, 'country_code' => 'PL'],
                    ['id' => 4, 'country_code' => 'PL'],
                    ['id' => 5, 'country_code' => 'US'],
                    ['id' => 6, 'country_code' => 'US'],
                    ['id' => 7, 'country_code' => 'US'],
                    ['id' => 9, 'country_code' => 'US'],
                ],
                schema(int_schema('id'), str_schema('country_code')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [['country_code' => 'PL', 'name' => 'Poland'], ['country_code' => 'US', 'name' => 'United States']],
                    schema(str_schema('country_code'), str_schema('name')),
                )),
                join_on(['country_code' => 'country_code']),
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 4, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 5, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 6, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 7, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 9, 'country_code' => 'US', 'name' => 'United States'],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_on_same_column_name(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'code' => 'PL'],
                    ['id' => 2, 'code' => 'PL'],
                    ['id' => 3, 'code' => 'PL'],
                    ['id' => 4, 'code' => 'PL'],
                    ['id' => 5, 'code' => 'US'],
                    ['id' => 6, 'code' => 'US'],
                    ['id' => 7, 'code' => 'US'],
                    ['id' => 9, 'code' => 'US'],
                ],
                schema(int_schema('id'), str_schema('code')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [['code' => 'PL', 'name' => 'Poland'], ['code' => 'US', 'name' => 'United States']],
                    schema(str_schema('code'), str_schema('name')),
                )),
                Expression::on(['code' => 'code'], 'joined_'),
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 4, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 5, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 6, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 7, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 9, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_right(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'US'],
                    ['id' => 3, 'country' => 'FR'],
                    ['id' => 4, 'country' => 'UK'],
                    ['id' => 5, 'country' => 'GB'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'FR', 'name' => 'France'],
                        ['code' => 'CA', 'name' => 'Canada'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                )),
                Expression::on(['country' => 'code'], 'joined_'),
                Join::right,
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'joined_code' => 'PL', 'joined_name' => 'Poland', 'country' => 'PL'],
                ['id' => 2, 'joined_code' => 'US', 'joined_name' => 'United States', 'country' => 'US'],
                ['id' => 3, 'joined_code' => 'FR', 'joined_name' => 'France', 'country' => 'FR'],
                ['id' => null, 'joined_code' => 'CA', 'joined_name' => 'Canada', 'country' => null],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_right_without_prefix(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->from(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country_code' => 'PL'],
                    ['id' => 2, 'country_code' => 'US'],
                    ['id' => 3, 'country_code' => 'FR'],
                    ['id' => 4, 'country_code' => 'UK'],
                    ['id' => 5, 'country_code' => 'GB'],
                ],
                schema(int_schema('id'), str_schema('country_code')),
            )))
            ->join(
                data_frame()->process(array_to_rows(
                    [
                        ['country_code' => 'PL', 'name' => 'Poland'],
                        ['country_code' => 'US', 'name' => 'United States'],
                        ['country_code' => 'FR', 'name' => 'France'],
                        ['country_code' => 'CA', 'name' => 'Canada'],
                    ],
                    schema(str_schema('country_code'), str_schema('name')),
                )),
                Expression::on(['country_code' => 'country_code']),
                Join::right,
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 3, 'country_code' => 'FR', 'name' => 'France'],
                ['id' => null, 'country_code' => 'CA', 'name' => 'Canada'],
            ],
            $rows->toArray(),
        );
    }

    public function test_a_date_key_meets_an_equal_datetime_key(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([['k' => new DateTimeImmutable('2026-01-02')]], schema(date_schema('k')))))
            ->join(
                df()->read(from_rows(array_to_rows([[
                    'r' => new DateTimeImmutable('2026-01-02 00:00:00 UTC'),
                ]], schema(datetime_schema('r'))))),
                join_on(['k' => 'r']),
                Join::inner,
            )
            ->fetch();

        static::assertCount(1, $rows);
    }
}
