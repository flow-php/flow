<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Constraint\SortedByConstraint;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Sort\Merge\KWayMerge;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_chunk;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function serialize;

final class KWayMergeTest extends FlowTestCase
{
    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 200; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    #[DataProvider('random_datasets')]
    public function test_merging_sorted_runs_equals_one_in_memory_sort(int $seed): void
    {
        $dataset = SortDatasetMother::random($seed);
        $buckets = new Buckets(new MemoryBuckets());
        $runs = [];

        foreach ($dataset['runs'] as $run => $rows) {
            foreach (array_to_rows($rows, $dataset['schema'])
                ->sortBy(...$dataset['references'])
                ->chunks($dataset['storedBatchSize']) as $batch) {
                $buckets->storage()->append("run-{$run}", $batch);
            }

            $runs[] = new BucketRun("run-{$run}", $buckets);
        }

        $merged = rows($dataset['schema']);

        foreach ((new KWayMerge(
            refs(...$dataset['references']),
            new AdaptiveBackend(),
            $dataset['mergeBatchSize'],
        ))->merge($runs) as $batch) {
            $merged = $merged->concat(
                new AdaptiveBackend(),
                $batch->matchTo($dataset['schema'], new AdaptiveBackend()),
            );
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                array_to_rows(array_merge(...$dataset['runs']), $dataset['schema'])
                    ->sortBy(...$dataset['references'])
                    ->toArray(),
            ),
            serialize($merged->toArray()),
        );
        static::assertNull((new SortedByConstraint(...$dataset['references']))->firstViolation($merged));
    }

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

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            array_column(
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('a', $buckets),
                    new BucketRun('b', $buckets),
                    new BucketRun('c', $buckets),
                ])),
                'id',
            ),
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

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            [1, 2, 3, 4],
            array_column(
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('sort-run-0', $spill),
                    new BucketRun('sort-merge-0', $merged),
                ])),
                'id',
            ),
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

        $merge = new KWayMerge(refs(ref('id')->desc()), new AdaptiveBackend());

        static::assertSame(
            [8, 7, 5, 4, 2, 1],
            array_column(
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('a', $buckets),
                    new BucketRun('b', $buckets),
                ])),
                'id',
            ),
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

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            [1, 2, 3, 4],
            array_column(
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('a', $buckets),
                    new BucketRun('b', $buckets),
                ])),
                'id',
            ),
        );
    }

    public function test_merges_non_numeric_values(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([['id' => 'a'], ['id' => 'c']], schema(str_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 'b'], ['id' => 'd']], schema(str_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            ['a', 'b', 'c', 'd'],
            array_column(
                BucketsStorageContext::rows($merge->merge([
                    new BucketRun('a', $buckets),
                    new BucketRun('b', $buckets),
                ])),
                'id',
            ),
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

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            BucketsStorageContext::rows($merge->merge([
                new BucketRun('a', $buckets),
                new BucketRun('b', $buckets),
            ])),
        );
    }

    public function test_single_run_passthrough(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->storage()->append('a', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

        static::assertSame(
            [1, 2],
            array_column(BucketsStorageContext::rows($merge->merge([new BucketRun('a', $buckets)])), 'id'),
        );
    }

    public function test_empty_bucket_set_yields_nothing(): void
    {
        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend());

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

        $merge = new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend(), batchSize: 2);

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
        new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend(), batchSize: 0);
    }

    public function test_interleaves_runs_across_batch_boundaries_like_one_sort(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $all = [];

        foreach (['a' => [2, 0], 'b' => [3, 1], 'c' => [5, 2]] as $run => [$batchSize, $offset]) {
            $rows = [];

            for ($i = 0; $i < 10; $i++) {
                $rows[] = ['id' => ($i * 3) + $offset, 'run' => $run];
            }

            $all = [...$all, ...$rows];

            foreach (array_chunk($rows, $batchSize) as $chunk) {
                $buckets->storage()->append($run, array_to_rows($chunk, schema(int_schema('id'), str_schema('run'))));
            }
        }

        $merged = [];

        foreach ((new KWayMerge(refs(ref('id')->asc()), new AdaptiveBackend(), batchSize: 4))->merge([
            new BucketRun('a', $buckets),
            new BucketRun('b', $buckets),
            new BucketRun('c', $buckets),
        ]) as $batch) {
            static::assertLessThanOrEqual(4, $batch->count());
            $merged = [...$merged, ...$batch->toArray()];
        }

        static::assertSame(
            array_to_rows($all, schema(int_schema('id'), str_schema('run')))->sortBy(ref('id')->asc())->toArray(),
            $merged,
        );
    }
}
