<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingHasher;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class SortedRunBucketingTest extends FlowTestCase
{
    public function test_bucket_ids_are_random_hex(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 2);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        foreach ($strategy->bucketize($generator) as $chunk) {
            static::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $chunk->bucketId);
        }
    }

    public function test_by_and_sorted_by_return_refs(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 2);

        static::assertEquals(refs('id'), $strategy->by());
        static::assertEquals(refs('id'), $strategy->sortedBy());
    }

    public function test_chunk_carries_values_and_hashes_aligned_with_rows(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 3);

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        foreach ($strategy->bucketize($generator) as $chunk) {
            static::assertCount($chunk->rows->count(), $chunk->hashes);
            static::assertCount($chunk->rows->count(), $chunk->values);
        }
    }

    public function test_emits_final_partial_run(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 2);

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $sizes = [];

        foreach ($strategy->bucketize($generator) as $chunk) {
            $sizes[] = $chunk->rows->count();
        }

        static::assertSame([2, 1], $sizes);
    }

    public function test_hashes_each_row_once_for_stats(): void
    {
        $spy = new CountingHasher(new NativeHasher());
        $strategy = new SortedRunBucketing(refs('id'), 2, $spy);

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        foreach ($strategy->bucketize($generator) as $_) {
        }

        static::assertSame(3, $spy->hashedRows());
    }

    public function test_runs_are_sorted_by_refs(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 3);

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $runs = [];

        foreach ($strategy->bucketize($generator) as $chunk) {
            $runs[] = $chunk->rows->toArray();
        }

        static::assertSame([[['id' => 1], ['id' => 2], ['id' => 3]]], $runs);
    }

    public function test_splits_buffer_into_runs_of_run_size(): void
    {
        $strategy = new SortedRunBucketing(refs('id'), 2);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $sizes = [];

        foreach ($strategy->bucketize($generator) as $chunk) {
            $sizes[] = $chunk->rows->count();
        }

        static::assertSame([2, 2], $sizes);
    }

    public function test_throws_when_run_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Run size must be greater than 0, given: 0');

        new SortedRunBucketing(refs('id'), 0);
    }
}
