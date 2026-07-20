<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\BucketStatsCollector;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\refs;

final class BucketStatsCollectorTest extends FlowTestCase
{
    public function test_bucket_passes_through_id_by_and_sorted_by(): void
    {
        $collector = new BucketStatsCollector(refs('id'));
        $collector->collect([['id' => 1]], ['a']);

        $bucket = $collector->bucket('bucket-1', refs('id'));

        static::assertSame('bucket-1', $bucket->id);
        static::assertEquals(refs('id'), $bucket->by);
        static::assertEquals(refs('id'), $bucket->stats->sortedBy());
    }

    public function test_counts_rows_and_chunks_across_collect_calls(): void
    {
        $collector = new BucketStatsCollector(refs('id'));
        $collector->collect([['id' => 1], ['id' => 2]], ['a', 'b']);
        $collector->collect([['id' => 3]], ['c']);

        $stats = $collector->bucket('bucket-1')->stats;

        static::assertSame(3, $stats->rowsCount());
        static::assertSame(2, $stats->chunksCount());
    }

    public function test_distinct_is_exact_below_cap(): void
    {
        $collector = new BucketStatsCollector(refs('id'), 10);
        $collector->collect([['id' => 1], ['id' => 1], ['id' => 2]], ['a', 'a', 'b']);

        $stats = $collector->bucket('bucket-1')->stats;

        static::assertSame(2, $stats->distinctEstimate());
        static::assertTrue($stats->distinctIsExact());
    }

    public function test_distinct_is_inexact_and_pinned_to_cap_when_exceeded(): void
    {
        $collector = new BucketStatsCollector(refs('id'), 2);
        $collector->collect([['id' => 1], ['id' => 2], ['id' => 3]], ['a', 'b', 'c']);

        $stats = $collector->bucket('bucket-1')->stats;

        static::assertSame(2, $stats->distinctEstimate());
        static::assertFalse($stats->distinctIsExact());
    }

    public function test_min_and_max_are_null_for_all_null_column(): void
    {
        $collector = new BucketStatsCollector(refs('id'));
        $collector->collect([['id' => null], ['id' => null]], ['a', 'a']);

        $stats = $collector->bucket('bucket-1')->stats;

        static::assertNull($stats->min('id'));
        static::assertNull($stats->max('id'));
        static::assertSame(2, $stats->nullCount('id'));
    }

    public function test_throws_when_distinct_cap_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Distinct cap must be greater than 0, given: 0');

        new BucketStatsCollector(refs('id'), 0);
    }

    public function test_tracks_min_max_and_null_count_per_column(): void
    {
        $collector = new BucketStatsCollector(refs('id', 'name'));
        $collector->collect([
            ['id' => 3, 'name' => 'b'],
            ['id' => 1, 'name' => 'a'],
            ['id' => 2, 'name' => null],
        ], ['a', 'b', 'c']);

        $stats = $collector->bucket('bucket-1')->stats;

        static::assertSame(1, $stats->min('id'));
        static::assertSame(3, $stats->max('id'));
        static::assertSame(0, $stats->nullCount('id'));
        static::assertSame('a', $stats->min('name'));
        static::assertSame('b', $stats->max('name'));
        static::assertSame(1, $stats->nullCount('name'));
    }
}
