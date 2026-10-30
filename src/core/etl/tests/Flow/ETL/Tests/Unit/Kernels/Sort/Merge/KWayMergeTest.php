<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort\Merge;

use Flow\ETL\Kernels\Bucketing\BucketRun;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\Merge\KWayMerge;
use Flow\ETL\Operators\Constraint\SortedByConstraint;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\Context\SortedRows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_chunk;
use function array_column;
use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
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
        $buckets = new Buckets(new MemoryRowsStorage());
        $runs = [];

        foreach ($dataset['runs'] as $run => $rows) {
            foreach (SortedRows::of(array_to_rows($rows, $dataset['schema']), ...$dataset['columns'])->chunks(
                $dataset['storedBatchSize'],
            ) as $batch) {
                $buckets->storage()->append("run-{$run}", $batch);
            }

            $runs[] = new BucketRun("run-{$run}", $buckets);
        }

        $merged = rows($dataset['schema']);

        foreach ((new KWayMerge(
            $dataset['columns'],
            new AdaptiveBackend(),
            new BatchSize($dataset['mergeBatchSize']),
        ))->merge(array_map(static fn(BucketRun $run): Generator => $run->rows(), $runs)) as $batch) {
            $merged = Rows::concat(
                $merged->schema(),
                new AdaptiveBackend(),
                $merged,
                $batch->matchTo($dataset['schema'], new AdaptiveBackend()),
            );
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                SortedRows::of(
                    array_to_rows(array_merge(...$dataset['runs']), $dataset['schema']),
                    ...$dataset['columns'],
                )->toArray(),
            ),
            serialize($merged->toArray()),
        );
        static::assertNull((new SortedByConstraint(...$dataset['columns']))->firstViolation($merged));
    }

    public function test_merges_plain_sorted_streams(): void
    {
        static::assertSame(
            [1, 2, 3, 4],
            array_column(
                RowsStorageContext::rows((new KWayMerge(
                    [ref('id')->asc()],
                    new AdaptiveBackend(),
                    new BatchSize(2),
                ))->merge([
                    (static function (): Generator {
                        yield array_to_rows([['id' => 1], ['id' => 4]], schema(int_schema('id')));
                    })(),
                    (static function (): Generator {
                        yield array_to_rows([['id' => 2], ['id' => 3]], schema(int_schema('id')));
                    })(),
                ])),
                'id',
            ),
        );
    }

    public function test_merges_multiple_sorted_runs_ascending(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
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

        static::assertSame(
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([
                    (new BucketRun('a', $buckets))->rows(),
                    (new BucketRun('b', $buckets))->rows(),
                    (new BucketRun('c', $buckets))->rows(),
                ])),
                'id',
            ),
        );
    }

    public function test_merge_reads_each_run_through_its_own_storage(): void
    {
        $spill = new Buckets(new MemoryRowsStorage());
        $merged = new Buckets(new MemoryRowsStorage());

        $spill->storage()->append('sort-run-0', array_to_rows([['id' => 1], ['id' => 3]], schema(int_schema('id'))));
        $merged->storage()->append('sort-merge-0', array_to_rows([['id' => 2], ['id' => 4]], schema(int_schema('id'))));

        // neither storage holds the other's id, so a routing miss reads 0 batches instead of throwing
        static::assertSame([], RowsStorageContext::rows($spill->storage()->get('sort-merge-0')));
        static::assertSame([], RowsStorageContext::rows($merged->storage()->get('sort-run-0')));

        static::assertSame(
            [1, 2, 3, 4],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([
                    (new BucketRun('sort-run-0', $spill))->rows(),
                    (new BucketRun('sort-merge-0', $merged))->rows(),
                ])),
                'id',
            ),
        );
    }

    public function test_merges_multiple_sorted_runs_descending(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
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

        static::assertSame(
            [8, 7, 5, 4, 2, 1],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref('id')->desc()], new AdaptiveBackend()))->merge([
                    (new BucketRun('a', $buckets))->rows(),
                    (new BucketRun('b', $buckets))->rows(),
                ])),
                'id',
            ),
        );
    }

    public function test_merges_runs_of_uneven_length(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 4]], schema(int_schema('id'))));

        static::assertSame(
            [1, 2, 3, 4],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([
                    (new BucketRun('a', $buckets))->rows(),
                    (new BucketRun('b', $buckets))->rows(),
                ])),
                'id',
            ),
        );
    }

    public function test_merges_non_numeric_values(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->storage()->append('a', array_to_rows([['id' => 'a'], ['id' => 'c']], schema(str_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 'b'], ['id' => 'd']], schema(str_schema('id'))));

        static::assertSame(
            ['a', 'b', 'c', 'd'],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([
                    (new BucketRun('a', $buckets))->rows(),
                    (new BucketRun('b', $buckets))->rows(),
                ])),
                'id',
            ),
        );
    }

    public function test_runs_under_different_schemas_are_conformed_to_the_first(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->storage()->append('a', array_to_rows(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name', true)),
        ));
        $buckets->storage()->append('b', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([
                (new BucketRun('a', $buckets))->rows(),
                (new BucketRun('b', $buckets))->rows(),
            ])),
        );
    }

    public function test_single_run_passthrough(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->storage()->append('a', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));

        static::assertSame(
            [1, 2],
            array_column(
                RowsStorageContext::rows((new KWayMerge([ref(
                    'id',
                )->asc()], new AdaptiveBackend()))->merge([(new BucketRun('a', $buckets))->rows()])),
                'id',
            ),
        );
    }

    public function test_empty_bucket_set_yields_nothing(): void
    {
        static::assertSame(
            [],
            RowsStorageContext::rows((new KWayMerge([ref('id')->asc()], new AdaptiveBackend()))->merge([])),
        );
    }

    public function test_merged_rows_are_yielded_in_batches_of_batch_size(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->storage()->append('a', array_to_rows([
            ['id' => 1],
            ['id' => 3],
            ['id' => 5],
        ], schema(int_schema('id'))));
        $buckets->storage()->append('b', array_to_rows([['id' => 2], ['id' => 4]], schema(int_schema('id'))));

        $merge = new KWayMerge([ref('id')->asc()], new AdaptiveBackend(), batchSize: new BatchSize(2));

        $sizes = [];

        foreach ($merge->merge([
            (new BucketRun('a', $buckets))->rows(),
            (new BucketRun('b', $buckets))->rows(),
        ]) as $batch) {
            $sizes[] = $batch->count();
        }

        static::assertSame([2, 2, 1], $sizes);
    }

    public function test_interleaves_runs_across_batch_boundaries_like_one_sort(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
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

        foreach ((new KWayMerge([ref('id')->asc()], new AdaptiveBackend(), batchSize: new BatchSize(4)))->merge([
            (new BucketRun('a', $buckets))->rows(),
            (new BucketRun('b', $buckets))->rows(),
            (new BucketRun('c', $buckets))->rows(),
        ]) as $batch) {
            static::assertLessThanOrEqual(4, $batch->count());
            $merged = [...$merged, ...$batch->toArray()];
        }

        static::assertSame(
            SortedRows::of(
                array_to_rows($all, schema(int_schema('id'), str_schema('run'))),
                ref('id')->asc(),
            )->toArray(),
            $merged,
        );
    }
}
