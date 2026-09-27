<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Sort\Merge\KWayMerge;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class KWayMergeTest extends FlowTestCase
{
    public function test_merges_multiple_sorted_runs_ascending(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 1],
            ['id' => 4],
            ['id' => 7],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([
            ['id' => 2],
            ['id' => 5],
            ['id' => 8],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('c', array_to_rows([
            ['id' => 3],
            ['id' => 6],
            ['id' => 9],
        ], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets), new BucketRun('b', $buckets), new BucketRun('c', $buckets)]))),
        );
    }

    public function test_merge_reads_each_run_through_its_own_storage(): void
    {
        $spill = new Buckets(new MemoryBuckets());
        $merged = new Buckets(new MemoryBuckets());

        $spill->storage()->append('sort-run-0', array_to_rows([['id' => 1], ['id' => 3]], schema(int_schema('id'))));
        $merged->storage()->append('sort-merge-0', array_to_rows([['id' => 2], ['id' => 4]], schema(int_schema('id'))));

        // neither storage holds the other's id, so a routing miss reads 0 batches instead of throwing
        static::assertSame([], BucketsStorageContext::rows($spill->storage()->get('sort-merge-0')));
        static::assertSame([], BucketsStorageContext::rows($merged->storage()->get('sort-run-0')));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            [1, 2, 3, 4],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([
                new BucketRun('sort-run-0', $spill),
                new BucketRun('sort-merge-0', $merged),
            ]))),
        );
    }

    public function test_merges_multiple_sorted_runs_descending(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 7],
            ['id' => 4],
            ['id' => 1],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([
            ['id' => 8],
            ['id' => 5],
            ['id' => 2],
        ], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->desc()));

        static::assertSame(
            [8, 7, 5, 4, 2, 1],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets), new BucketRun('b', $buckets)]))),
        );
    }

    public function test_merges_runs_of_uneven_length(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 4]], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            [1, 2, 3, 4],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets), new BucketRun('b', $buckets)]))),
        );
    }

    public function test_merges_non_numeric_values(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([['id' => 'a'], ['id' => 'c']], schema(str_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 'b'], ['id' => 'd']], schema(str_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            ['a', 'b', 'c', 'd'],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets), new BucketRun('b', $buckets)]))),
        );
    }

    public function test_runs_under_different_schemas_are_conformed_to_the_first(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name', true)),
        ));
        $buckets->storage()->append('b', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            array_map(
                static fn(Row $r): array => $r->toArray(),
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('a', $buckets),
                    new BucketRun('b', $buckets),
                ])),
            ),
        );
    }

    public function test_single_run_passthrough(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame(
            [1, 2],
            array_map(static fn(Row $r): mixed => $r->get(
                'id',
            ), BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets)]))),
        );
    }

    public function test_empty_bucket_set_yields_nothing(): void
    {
        $merge = new KWayMerge(refs(ref('id')->asc()));

        static::assertSame([], BucketsStorageContext::rows($merge->merge([])));
    }

    public function test_merged_rows_are_yielded_in_batches_of_batch_size(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 1],
            ['id' => 3],
            ['id' => 5],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 2], ['id' => 4]], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()), batchSize: 2);

        $sizes = [];

        foreach ($merge->merge([new BucketRun('a', $buckets), new BucketRun('b', $buckets)]) as $batch) {
            $sizes[] = $batch->count();
        }

        static::assertSame([2, 2, 1], $sizes);
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new KWayMerge(refs(ref('id')->asc()), batchSize: 0);
    }
}
