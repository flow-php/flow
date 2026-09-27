<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\Double\ThrowingRemoveBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;
use function range;

final class MergeSortProcessorTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $storage = new MemoryBuckets();
        $input = schema(int_schema('id'));
        $processor = new MergeSortProcessor(
            refs(ref('id')),
            new Buckets($storage),
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
        );

        static::assertEquals($input, $processor->bind($input)->output);
    }

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
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            10,
            1000,
        );

        $input = (static function () {
            yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]], schema(int_schema('id')));
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
        $merge = new MergeSortProcessor(
            refs('id'),
            $buckets,
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            10,
            1000,
        );

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
        $merge = new MergeSortProcessor(
            refs('id'),
            $buckets,
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            10,
            1000,
        );

        $input = (static function () {
            yield array_to_rows(array_map(static fn(int $i): array => ['id' => $i], [
                0,
                10,
                1,
                11,
                2,
                12,
                3,
                13,
            ]), schema(int_schema('id')));
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
        $merge = new MergeSortProcessor(
            refs('a', 'b'),
            $buckets,
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            10,
            1000,
        );

        $input = (static function () {
            yield array_to_rows(
                [['a' => 1, 'b' => 2], ['a' => 1, 'b' => 1], ['a' => 0, 'b' => 5], ['a' => 0, 'b' => 3]],
                schema(int_schema('a'), int_schema('b')),
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
        $merge = new MergeSortProcessor(
            refs('id'),
            $buckets,
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            2,
            1000,
        );

        $input = (static function () {
            yield array_to_rows(
                array_map(static fn(int $i): array => ['id' => $i], range(19, 0)),
                schema(int_schema('id')),
            );
        })();

        $result = iterator_to_array($merge->process($bucketing->process($input, $context), $context), false);

        static::assertSame(
            range(0, 19),
            array_merge(...array_map(static fn(Rows $r): array => $r->reduceToArray('id'), $result)),
        );
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_both_storages_are_cleared_when_the_first_clear_throws(): void
    {
        $spillStorage = new SpyBucketsStorage(new MemoryBuckets());
        $mergeStorage = new SpyBucketsStorage(new MemoryBuckets());
        $spill = new Buckets(new ThrowingRemoveBucketsStorage($spillStorage));
        $merge = new Buckets($mergeStorage);
        $context = flow_context(config());

        // the upstream throws during the drain, so reduce() never writes a merged run - seed one, or the
        // merge-side assertion is true whether or not the nested finally ran
        $mergeStorage->append('pre-existing', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $merge->add(new Bucket('pre-existing', 1, 0));

        $bucketing = new BucketingProcessor(
            new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator()),
            $spill,
        );
        $processor = new MergeSortProcessor(refs('id'), $spill, $merge, new NativePHPRandomValueGenerator(), 2, 1000);

        $upstream = (static function (): Generator {
            yield array_to_rows(
                array_map(static fn(int $i): array => ['id' => $i], range(9, 0)),
                schema(int_schema('id')),
            );

            throw new RuntimeException('upstream failed');
        })();

        try {
            iterator_to_array($processor->process($bucketing->process($upstream, $context), $context), false);
            static::fail('The upstream failure must escape.');
        } catch (RuntimeException $escaped) {
            static::assertSame('spill clear failed', $escaped->getMessage());

            $previous = $escaped->getPrevious();

            static::assertInstanceOf(RuntimeException::class, $previous);
            static::assertSame('upstream failed', $previous->getMessage());
        }

        // the nested finally runs the merge clear even though the spill clear threw
        static::assertSame([], $mergeStorage->liveBucketIds());
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new MergeSortProcessor(
            refs('id'),
            new Buckets(new MemoryBuckets()),
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
            new Buckets(new MemoryBuckets()),
            new NativePHPRandomValueGenerator(),
            0,
            1000,
        );
    }
}
