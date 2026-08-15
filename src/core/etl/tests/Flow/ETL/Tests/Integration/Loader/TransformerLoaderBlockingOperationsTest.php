<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Join\Join;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function array_column;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\window;

/**
 * A Transformation given to to_transformation() expands into a nested DataFrame that is re-driven once per incoming
 * batch, over a source yielding exactly that one batch. Anything expanding to a Processor therefore answers for a
 * single batch instead of the stream.
 *
 * The characterization tests below assert today's KNOWN-WRONG results on purpose, so that a future change to those
 * semantics fails loudly instead of silently. They are the baseline for
 * .claude/tasks/transformation-loader-bug/04-transformation-blocking-ops.md - do not "fix" their assertions.
 */
final class TransformerLoaderBlockingOperationsTest extends FlowIntegrationTestCase
{
    public function test_aggregate_inside_a_transformation_is_batch_local(): void
    {
        // CHARACTERIZATION (A2) - each batch is summed on its own. Correct is a single row {v_sum: 66}.
        $spy = new SpyLoader();
        $sumV = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->aggregate(sum(ref('v'))));

        df()->read(from_rows(...RowsMother::interleavedGroupBatches()))->write(to_transformation($sumV, $spy))->run();

        static::assertSame([['v_sum' => 11], ['v_sum' => 22], ['v_sum' => 33]], $spy->loadedRowsToArray());
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_batch_by_inside_a_transformation_does_not_honour_batch_boundaries(): void
    {
        // CHARACTERIZATION (A7) - the mildest case: every row arrives, in the right order, but the chunks are cut at
        // the incoming batch boundaries instead of at the group boundaries. Correct chunking is [3, 3].
        $spy = new SpyLoader();
        $batchByGroup = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->batchBy(ref('g')));

        df()
            ->read(from_rows(...RowsMother::sortedGroupBatches()))
            ->write(to_transformation($batchByGroup, $spy))
            ->run();

        static::assertSame([2, 1, 1, 2], $spy->loadedRowCounts());
        static::assertSame(
            [
                ['g' => 'a', 'v' => 1],
                ['g' => 'a', 'v' => 2],
                ['g' => 'a', 'v' => 3],
                ['g' => 'b', 'v' => 10],
                ['g' => 'b', 'v' => 20],
                ['g' => 'b', 'v' => 30],
            ],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_cache_inside_a_transformation_persists_only_the_last_batch(): void
    {
        // CHARACTERIZATION (A9) - DATA LOSS. Counting loads proves nothing here, the run reports all 6 rows; the
        // damage is only visible by reading the cache back. Correct is 6 readable rows, ->collect() on the outer
        // frame produces them.
        $spy = new SpyLoader();
        $cacheRows = new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->cache('transformation-blocking-operations'),
        );

        df()->read(from_rows(...RowsMother::sortedGroupBatches()))->write(to_transformation($cacheRows, $spy))->run();

        static::assertCount(6, $spy->loadedRowsToArray());
        static::assertSame(2, df()->read(from_cache('transformation-blocking-operations'))->count());
    }

    public function test_collect_inside_a_transformation_does_not_collect(): void
    {
        // CHARACTERIZATION (A10) - collect() inside the transformation collects the single batch it was handed, so it
        // is a no-op. Correct is one load of 6 rows, reachable only by collecting the OUTER frame.
        $spy = new SpyLoader();
        $collectRows = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect());

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation($collectRows, $spy))
            ->run();

        static::assertSame([2, 2, 2], $spy->loadedRowCounts());
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_drop_duplicates_inside_a_transformation_deduplicates_globally(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. DropDuplicatesTransformer is a Transformer holding its
        // hashes, and the memoized nested frame keeps it alive across batches.
        $spy = new SpyLoader();
        $dedupByGroup = new CallbackTransformation(static function (DataFrame $df): DataFrame {
            return $df->dropDuplicates(ref('g'));
        });

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($dedupByGroup, $spy))
            ->run();

        static::assertSame([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 10]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_group_by_aggregate_inside_a_transformation_is_batch_local(): void
    {
        // CHARACTERIZATION (A3) - groups are never merged across batches. Correct is 2 rows, (a, 6) and (b, 60).
        $spy = new SpyLoader();
        $sumVByGroup = new CallbackTransformation(static function (DataFrame $df): DataFrame {
            return $df->groupBy(ref('g'))->aggregate(sum(ref('v')));
        });

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($sumVByGroup, $spy))
            ->run();

        static::assertSame(
            [
                ['g' => 'a', 'v_sum' => 1],
                ['g' => 'b', 'v_sum' => 10],
                ['g' => 'a', 'v_sum' => 2],
                ['g' => 'b', 'v_sum' => 20],
                ['g' => 'a', 'v_sum' => 3],
                ['g' => 'b', 'v_sum' => 30],
            ],
            $spy->loadedRowsToArray(),
        );
        static::assertSame(6, $spy->loadsCount);
    }

    public function test_join_inside_a_transformation_matches_every_row(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. HashJoinProcessor buffers the RIGHT side, which is a separate
        // complete frame, and streams the left side row by row, so the match is row-local. Only the chunking differs
        // from the outer frame (6 loads of 1 against 2 of 3), which is why loadsCount is deliberately not asserted.
        $spy = new SpyLoader();
        $joinNames = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->join(
            data_frame()->process(rows(
                row(str_entry('code', 'a'), str_entry('n', 'Alpha')),
                row(str_entry('code', 'b'), str_entry('n', 'Bravo')),
            )),
            join_on(['g' => 'code'], 'j_'),
            Join::inner,
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($joinNames, $spy))
            ->run();

        static::assertSame(
            [
                ['g' => 'a', 'v' => 1, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'b', 'v' => 10, 'j_code' => 'b', 'j_n' => 'Bravo'],
                ['g' => 'a', 'v' => 2, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'b', 'v' => 20, 'j_code' => 'b', 'j_n' => 'Bravo'],
                ['g' => 'a', 'v' => 3, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'b', 'v' => 30, 'j_code' => 'b', 'j_n' => 'Bravo'],
            ],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_limit_inside_a_transformation_applies_across_the_whole_stream(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. LimitReachedException round-trips into Signal::STOP, which
        // stops the swappable source and suppresses further nested runs. Identical to limiting the outer frame.
        $spy = new SpyLoader();
        $limitToThree = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->limit(3));

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation($limitToThree, $spy))
            ->run();

        static::assertSame([5, 4, 3], array_column($spy->loadedRowsToArray(), 'id'));
        static::assertSame([2, 1], $spy->loadedRowCounts());
    }

    public function test_offset_inside_a_transformation_loses_every_row(): void
    {
        // CHARACTERIZATION (A8) - DATA LOSS. Each nested run skips its offset within a single batch, and every batch
        // is smaller than the offset, so every batch is consumed entirely and load() is never called. Correct is one
        // load of 4 rows, [3, 2, 1, 0]. The pipeline reports success and writes nothing.
        $spy = new SpyLoader();
        $skipTwo = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->offset(2));

        df()->read(from_rows(...RowsMother::descendingIdBatches()))->write(to_transformation($skipTwo, $spy))->run();

        static::assertSame(0, $spy->loadsCount);
        static::assertSame([], $spy->loadedRows);
    }

    public function test_outer_collect_makes_a_nested_offset_load_every_expected_row(): void
    {
        // WORKAROUND PROOF - the documentation tells users to collect the OUTER frame; this pins that it works.
        $spy = new SpyLoader();
        $skipTwo = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->offset(2));

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->collect()
            ->write(to_transformation($skipTwo, $spy))
            ->run();

        static::assertSame([3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_outer_collect_makes_a_nested_sort_by_globally_correct(): void
    {
        // WORKAROUND PROOF - the documentation tells users to collect the OUTER frame; this pins that it works.
        $spy = new SpyLoader();
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->collect()
            ->write(to_transformation($sortById, $spy))
            ->run();

        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_pivot_inside_a_transformation_is_batch_local(): void
    {
        // CHARACTERIZATION (A6) - one pivoted set per batch. Correct is 2 rows, {g: a, a: 6, b: null} and
        // {g: b, b: 60, a: null}.
        $spy = new SpyLoader();
        $pivotGroupSums = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df
            ->groupBy(ref('g'))
            ->pivot(ref('g'))
            ->aggregate(sum(ref('v'))));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($pivotGroupSums, $spy))
            ->run();

        static::assertSame(
            [
                ['g' => 'a', 'a' => 1, 'b' => null],
                ['g' => 'b', 'b' => 10, 'a' => null],
                ['g' => 'a', 'a' => 2, 'b' => null],
                ['g' => 'b', 'b' => 20, 'a' => null],
                ['g' => 'a', 'a' => 3, 'b' => null],
                ['g' => 'b', 'b' => 30, 'a' => null],
            ],
            $spy->loadedRowsToArray(),
        );
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_row_number_inside_a_transformation_restarts_every_batch(): void
    {
        // CHARACTERIZATION (A5) - the window covers one batch, so the numbering restarts. Correct is [1..6] in one
        // load of 6 rows.
        $spy = new SpyLoader();
        $numberByValue = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withEntry(
            'rn',
            row_number()->over(window()->orderBy(ref('v'))),
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($numberByValue, $spy))
            ->run();

        static::assertSame([1, 2, 1, 2, 1, 2], array_column($spy->loadedRowsToArray(), 'rn'));
        static::assertSame([2, 2, 2], $spy->loadedRowCounts());
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_sort_by_inside_a_transformation_is_batch_local(): void
    {
        // CHARACTERIZATION (A1) - each batch is sorted on its own. Correct is [0, 1, 2, 3, 4, 5] in a single load.
        $spy = new SpyLoader();
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));

        df()->read(from_rows(...RowsMother::descendingIdBatches()))->write(to_transformation($sortById, $spy))->run();

        static::assertSame([4, 5, 2, 3, 0, 1], array_column($spy->loadedRowsToArray(), 'id'));
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_until_inside_a_transformation_applies_across_the_whole_stream(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. UntilTransformer is a Transformer and its STOP propagates out
        // of the nested frame. Identical to running until() on the outer frame.
        $spy = new SpyLoader();
        $untilValueReachesThree = new CallbackTransformation(static function (DataFrame $df): DataFrame {
            return $df->until(ref('v')->lessThan(lit(3)));
        });

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($untilValueReachesThree, $spy))
            ->run();

        static::assertSame([['g' => 'a', 'v' => 1]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_window_partition_by_inside_a_transformation_is_batch_local(): void
    {
        // CHARACTERIZATION (A4) - each partition holds the single row its batch contributed, so every average equals
        // that row's own value. Correct is avg 2 for group a and avg 20 for group b, in 2 loads of 3 rows.
        $spy = new SpyLoader();
        $averageOverGroup = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withEntry(
            'avg',
            average(ref('v'))->over(window()->partitionBy(ref('g'))),
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($averageOverGroup, $spy))
            ->run();

        static::assertSame([1, 10, 2, 20, 3, 30], array_column($spy->loadedRowsToArray(), 'avg'));
        static::assertSame(6, $spy->loadsCount);
    }
}
