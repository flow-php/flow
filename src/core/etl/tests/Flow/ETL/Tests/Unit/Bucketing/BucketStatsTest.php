<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\BucketStats;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketStatsMother;

use function Flow\ETL\DSL\refs;

final class BucketStatsTest extends FlowTestCase
{
    public function test_counts_and_min_max_pass_through(): void
    {
        $stats = new BucketStats(
            rowsCount: 10,
            chunksCount: 3,
            min: ['id' => 1],
            max: ['id' => 100],
            nullCounts: ['id' => 2],
            distinctEstimate: 5,
            distinctExact: true,
        );

        static::assertSame(10, $stats->rowsCount());
        static::assertSame(3, $stats->chunksCount());
        static::assertSame(1, $stats->min('id'));
        static::assertSame(100, $stats->max('id'));
        static::assertSame(2, $stats->nullCount('id'));
        static::assertSame(5, $stats->distinctEstimate());
        static::assertTrue($stats->distinctIsExact());
    }

    public function test_max_throws_for_untracked_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No max statistic tracked for column "id".');

        BucketStatsMother::with(min: ['id' => 1])->max('id');
    }

    public function test_min_and_max_return_null_for_all_null_column(): void
    {
        $stats = BucketStatsMother::with(min: ['id' => null], max: ['id' => null], nullCounts: ['id' => 4]);

        static::assertNull($stats->min('id'));
        static::assertNull($stats->max('id'));
    }

    public function test_min_throws_for_untracked_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No min statistic tracked for column "id".');

        BucketStatsMother::with()->min('id');
    }

    public function test_null_count_defaults_to_zero_for_untracked_column(): void
    {
        static::assertSame(0, BucketStatsMother::with()->nullCount('id'));
    }

    public function test_skew_is_null_when_distinct_estimate_is_null(): void
    {
        static::assertNull(BucketStatsMother::with(rowsCount: 10, distinctEstimate: null)->skew());
    }

    public function test_skew_is_null_when_distinct_estimate_is_zero(): void
    {
        static::assertNull(BucketStatsMother::with(rowsCount: 10, distinctEstimate: 0)->skew());
    }

    public function test_skew_is_rows_count_over_distinct_estimate(): void
    {
        static::assertSame(4.0, BucketStatsMother::with(rowsCount: 20, distinctEstimate: 5)->skew());
    }

    public function test_sorted_by_pass_through(): void
    {
        static::assertNull(BucketStatsMother::with()->sortedBy());
        static::assertEquals(refs('id'), BucketStatsMother::with(sortedBy: refs('id'))->sortedBy());
    }
}
