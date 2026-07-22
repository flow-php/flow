<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Row;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_unique;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;

final class SortedRunBucketingTest extends FlowTestCase
{
    public function test_bucket_ids_are_namespaced_per_run(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $ids = [];

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            static::assertMatchesRegularExpression('/^sort-[0-9a-f]{16}-\d+$/', $bucket->id);
            $ids[] = $bucket->id;
        }

        static::assertCount(2, $ids);
        static::assertSame($ids, array_unique($ids));
    }

    public function test_emits_final_partial_run(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $sizes = [];

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            $sizes[] = $bucket->totalRows;
        }

        static::assertSame([2, 1], $sizes);
    }

    public function test_runs_are_spilled_sorted_by_refs(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 3, new NativePHPRandomValueGenerator());
        $storage = new MemoryBuckets();

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $buckets = iterator_to_array($strategy->bucketize($generator, $storage));

        static::assertCount(1, $buckets);
        static::assertSame(
            [1, 2, 3],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get($buckets[0]->id))),
        );
    }

    public function test_runs_are_yielded_as_soon_as_they_are_complete(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator());
        $storage = new MemoryBuckets();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $firstRun = $strategy->bucketize($generator, $storage)->current();

        static::assertNotNull($firstRun);
        static::assertSame(2, $firstRun->totalRows);
        static::assertCount(2, BucketsStorageContext::rows($storage->get($firstRun->id)));
    }

    public function test_splits_buffer_into_runs_of_run_size(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $sizes = [];

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            $sizes[] = $bucket->totalRows;
        }

        static::assertSame([2, 2], $sizes);
    }

    public function test_runs_are_numbered_sequentially(): void
    {
        $strategy = new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
            yield rows(row(int_entry('id', 5)));
        })();

        $indexes = [];

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            $indexes[] = $bucket->index;
        }

        static::assertSame([0, 1, 2], $indexes);
    }

    public function test_throws_when_run_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Run size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new SortedRunBucketing([ref('id')], 0, new NativePHPRandomValueGenerator());
    }
}
