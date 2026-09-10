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

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'US']),
                row(['id' => 3, 'country' => 'FR']),
                row(['id' => 4, 'country' => 'UK']),
                row(['id' => 5, 'country' => 'GB']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
                    row(['code' => 'FR', 'name' => 'France']),
                    row(['code' => 'CN', 'name' => 'Canada']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'US']),
                row(['id' => 3, 'country' => 'FR']),
                row(['id' => 4, 'country' => 'UK']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
                    row(['code' => 'FR', 'name' => 'France']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country'), str_schema('note', nullable: true)),
                row(['id' => 1, 'country' => 'PL', 'note' => 'has-note']),
                row(['id' => 2, 'country' => 'US', 'note' => null]),
                row(['id' => 3, 'country' => 'FR', 'note' => 'third']),
                row(['id' => 4, 'country' => 'PL', 'note' => null]),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
                    row(['code' => 'FR', 'name' => 'France']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'PL']),
                row(['id' => 3, 'country' => 'PL']),
                row(['id' => 4, 'country' => 'PL']),
                row(['id' => 5, 'country' => 'US']),
                row(['id' => 6, 'country' => 'US']),
                row(['id' => 7, 'country' => 'US']),
                row(['id' => 9, 'country' => 'US']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'US']),
                row(['id' => 3, 'country' => 'FR']),
                row(['id' => 5, 'country' => 'GB']),
                row(['id' => 7, 'country' => 'CN']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
                    row(['code' => 'FR', 'name' => 'France']),
                    row(['code' => 'CA', 'name' => 'Canada']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), datetime_schema('date')),
                row(['id' => 1, 'date' => new DateTimeImmutable('2024-01-01 00:00:00')]),
                row(['id' => 2, 'date' => new DateTimeImmutable('2024-01-01 00:00:00')]),
                row(['id' => 3, 'date' => new DateTimeImmutable('2024-01-02 00:00:00')]),
                row(['id' => 4, 'date' => new DateTimeImmutable('2024-01-03 00:00:00')]),
                row(['id' => 5, 'date' => new DateTimeImmutable('2024-01-04 00:00:00')]),
                row(['id' => 6, 'date' => new DateTimeImmutable('2024-01-04 00:00:00')]),
                row(['id' => 7, 'date' => new DateTimeImmutable('2024-01-05 00:00:00')]),
                row(['id' => 9, 'date' => new DateTimeImmutable('2024-01-05 00:00:00')]),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(datetime_schema('date'), int_schema('events')),
                    row(['date' => new DateTimeImmutable('2024-01-01 00:00:00'), 'events' => 1]),
                    row(['date' => new DateTimeImmutable('2024-01-05 00:00:00'), 'events' => 5]),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country_code')),
                row(['id' => 1, 'country_code' => 'PL']),
                row(['id' => 2, 'country_code' => 'PL']),
                row(['id' => 3, 'country_code' => 'PL']),
                row(['id' => 4, 'country_code' => 'PL']),
                row(['id' => 5, 'country_code' => 'US']),
                row(['id' => 6, 'country_code' => 'US']),
                row(['id' => 7, 'country_code' => 'US']),
                row(['id' => 9, 'country_code' => 'US']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('country_code'), str_schema('name')),
                    row(['country_code' => 'PL', 'name' => 'Poland']),
                    row(['country_code' => 'US', 'name' => 'United States']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('code')),
                row(['id' => 1, 'code' => 'PL']),
                row(['id' => 2, 'code' => 'PL']),
                row(['id' => 3, 'code' => 'PL']),
                row(['id' => 4, 'code' => 'PL']),
                row(['id' => 5, 'code' => 'US']),
                row(['id' => 6, 'code' => 'US']),
                row(['id' => 7, 'code' => 'US']),
                row(['id' => 9, 'code' => 'US']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'US']),
                row(['id' => 3, 'country' => 'FR']),
                row(['id' => 4, 'country' => 'UK']),
                row(['id' => 5, 'country' => 'GB']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('code'), str_schema('name')),
                    row(['code' => 'PL', 'name' => 'Poland']),
                    row(['code' => 'US', 'name' => 'United States']),
                    row(['code' => 'FR', 'name' => 'France']),
                    row(['code' => 'CA', 'name' => 'Canada']),
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
            ->from(from_rows(rows(
                schema(int_schema('id'), str_schema('country_code')),
                row(['id' => 1, 'country_code' => 'PL']),
                row(['id' => 2, 'country_code' => 'US']),
                row(['id' => 3, 'country_code' => 'FR']),
                row(['id' => 4, 'country_code' => 'UK']),
                row(['id' => 5, 'country_code' => 'GB']),
            )))
            ->join(
                data_frame()->process(rows(
                    schema(str_schema('country_code'), str_schema('name')),
                    row(['country_code' => 'PL', 'name' => 'Poland']),
                    row(['country_code' => 'US', 'name' => 'United States']),
                    row(['country_code' => 'FR', 'name' => 'France']),
                    row(['country_code' => 'CA', 'name' => 'Canada']),
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
}
