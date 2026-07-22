<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;
use function range;

final class MergeSortProcessorTest extends FlowTestCase
{
    public function test_descending_single_column(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $context = flow_context(config());

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('id')->desc()], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );
        $merge = new MergeSortProcessor(
            refs(ref('id')->desc()),
            $buckets,
            new NativePHPRandomValueGenerator(),
            10,
            1000,
        );

        $input = (static function () {
            yield rows(
                row(int_entry('id', 3)),
                row(int_entry('id', 1)),
                row(int_entry('id', 4)),
                row(int_entry('id', 2)),
            );
        })();

        $result = iterator_to_array($merge->process($bucketing->process($input, $context), $context), false);

        static::assertSame(
            [4, 3, 2, 1],
            array_merge(...array_map(static fn(Rows $r): array => $r->reduceToArray('id'), $result)),
        );
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_empty_input_yields_nothing_and_leaves_storage_empty(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $context = flow_context(config());

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );
        $merge = new MergeSortProcessor(refs('id'), $buckets, new NativePHPRandomValueGenerator(), 10, 1000);

        $input = (static function () {
            yield from [];
        })();

        static::assertCount(0, iterator_to_array(
            $merge->process($bucketing->process($input, $context), $context),
            false,
        ));
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_heap_merges_overlapping_runs_without_reduction(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $context = flow_context(config());

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );
        $merge = new MergeSortProcessor(refs('id'), $buckets, new NativePHPRandomValueGenerator(), 10, 1000);

        $input = (static function () {
            yield rows(...array_map(static fn(int $i): Row => row(int_entry('id', $i)), [0, 10, 1, 11, 2, 12, 3, 13]));
        })();

        $result = iterator_to_array($merge->process($bucketing->process($input, $context), $context), false);

        static::assertSame(
            [0, 1, 2, 3, 10, 11, 12, 13],
            array_merge(...array_map(static fn(Rows $r): array => $r->reduceToArray('id'), $result)),
        );
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_sorts_by_multiple_columns(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $context = flow_context(config());

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('a'), ref('b')], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );
        $merge = new MergeSortProcessor(refs('a', 'b'), $buckets, new NativePHPRandomValueGenerator(), 10, 1000);

        $input = (static function () {
            yield rows(
                row(int_entry('a', 1), int_entry('b', 2)),
                row(int_entry('a', 1), int_entry('b', 1)),
                row(int_entry('a', 0), int_entry('b', 5)),
                row(int_entry('a', 0), int_entry('b', 3)),
            );
        })();

        $result = iterator_to_array($merge->process($bucketing->process($input, $context), $context), false);

        static::assertSame(
            [
                ['a' => 0, 'b' => 3],
                ['a' => 0, 'b' => 5],
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            array_merge(...array_map(static fn(Rows $r): array => $r->toArray(), $result)),
        );
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_multi_pass_reduction_produces_sorted_output(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $context = flow_context(config());

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );
        $merge = new MergeSortProcessor(refs('id'), $buckets, new NativePHPRandomValueGenerator(), 2, 1000);

        $input = (static function () {
            yield rows(...array_map(static fn(int $i): Row => row(int_entry('id', $i)), range(19, 0)));
        })();

        $result = iterator_to_array($merge->process($bucketing->process($input, $context), $context), false);

        static::assertSame(
            range(0, 19),
            array_merge(...array_map(static fn(Rows $r): array => $r->reduceToArray('id'), $result)),
        );
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new MergeSortProcessor(
            refs('id'),
            new Buckets(new MemoryBuckets()),
            new NativePHPRandomValueGenerator(),
            10,
            0,
        );
    }

    public function test_throws_when_merge_fan_in_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merge fan-in must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new MergeSortProcessor(
            refs('id'),
            new Buckets(new MemoryBuckets()),
            new NativePHPRandomValueGenerator(),
            0,
            1000,
        );
    }
}
