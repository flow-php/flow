<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\ConstantHasher;
use Flow\ETL\Tests\Double\CountingHasher;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class HashBucketingTest extends FlowTestCase
{
    public function test_bucket_ids_carry_namespace_and_run_id(): void
    {
        $strategy = new HashBucketing(refs('id'), 4, namespace: 'group-by');

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
        })();

        foreach ($strategy->bucketize($generator) as $chunk) {
            static::assertMatchesRegularExpression('/^group-by-[0-9a-f]{16}-\d$/', $chunk->bucketId);
        }
    }

    public function test_by_returns_bucket_columns_and_sorted_by_is_null(): void
    {
        $strategy = new HashBucketing(refs('id'), 4);

        static::assertEquals(refs('id'), $strategy->by());
        static::assertNull($strategy->sortedBy());
    }

    public function test_chunk_carries_values_and_hashes_aligned_with_rows(): void
    {
        $strategy = new HashBucketing(refs('id'), 4);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
            );
        })();

        foreach ($strategy->bucketize($generator) as $chunk) {
            static::assertCount($chunk->rows->count(), $chunk->hashes);
            static::assertCount($chunk->rows->count(), $chunk->values);
        }
    }

    public function test_custom_hasher_overrides_assignment(): void
    {
        $strategy = new HashBucketing(refs('id'), 8, new ConstantHasher('00000000ffffffff'));

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)));
        })();

        $ids = [];

        foreach ($strategy->bucketize($generator) as $chunk) {
            $ids[$chunk->bucketId] = true;
        }

        static::assertCount(1, $ids);
    }

    public function test_distributes_rows_within_buckets_count(): void
    {
        $strategy = new HashBucketing(refs('id'), 4);

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = row(int_entry('id', $i));
        }

        $generator = (static function () use ($rows) {
            yield new Rows(...$rows);
        })();

        $total = 0;

        foreach ($strategy->bucketize($generator) as $chunk) {
            static::assertMatchesRegularExpression('/-[0-3]$/', $chunk->bucketId);
            $total += $chunk->rows->count();
        }

        static::assertSame(100, $total);
    }

    public function test_hashes_each_row_exactly_once(): void
    {
        $spy = new CountingHasher(new NativeHasher());
        $strategy = new HashBucketing(refs('id'), 4, $spy);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
        })();

        foreach ($strategy->bucketize($generator) as $_) {
        }

        static::assertSame(3, $spy->hashedRows());
    }

    public function test_throws_when_buckets_count_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Buckets count must be greater than 0, given: 0');

        new HashBucketing(refs('id'), 0);
    }
}
