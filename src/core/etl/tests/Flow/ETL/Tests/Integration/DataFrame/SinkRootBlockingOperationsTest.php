<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Join\Join;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use RuntimeException;

use function array_column;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\constraint_unique;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\window;

/**
 * A Transformation given to to_transformation() becomes a sink root: one side pipeline per run, fed batch by batch
 * and drained when the run ends. A Processor placed there therefore answers exactly as it would on the outer frame,
 * and a blocking operation buffers proportional to the data - the same cost it has outside a Transformation.
 *
 * The values below are the outer-frame ground truth: running the same operation on the outer frame produces them
 * byte for byte. If one of them starts failing, the sink root regressed - do not "fix" the assertion.
 *
 * See documentation/components/core/transformations.md.
 */
final class SinkRootBlockingOperationsTest extends FlowIntegrationTestCase
{
    public function test_a_drain_failure_under_skip_rows_completes_and_drops_the_whole_buffered_batch(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(2)
            ->onError(skip_rows_handler())
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->with(
                    new ThrowWhenRowMatches('id', 3, new RuntimeException('boom')),
                )),
                $spy,
            ))
            ->run();

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_aggregate_inside_a_transformation_aggregates_the_whole_stream(): void
    {
        // One result row for the stream, not one per batch.
        $spy = new SpyLoader();
        $sumV = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->aggregate([sum(ref('v'))]));

        df()->read(from_rows(...RowsMother::interleavedGroupBatches()))->write(to_transformation($sumV, $spy))->run();

        static::assertSame([['v_sum' => 66.0]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_batch_by_inside_a_transformation_batches_by_group_across_the_stream(): void
    {
        // Chunks are cut at the group boundaries of the whole stream, not of each source batch.
        $spy = new SpyLoader();
        $batchByGroup = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->batchBy(ref('g')));

        df()
            ->read(from_rows(...RowsMother::sortedGroupBatches()))
            ->write(to_transformation($batchByGroup, $spy))
            ->run();

        static::assertSame([3, 3], $spy->loadedRowCounts());
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

    public function test_cache_inside_a_transformation_persists_the_whole_stream(): void
    {
        // Counting loads proves nothing here, the run always reported all 6 rows; the cache read-back is what
        // shows the whole stream was persisted under the user-chosen id.
        $spy = new SpyLoader();
        $cacheRows = new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->cache('transformation-blocking-operations'),
        );

        df()->read(from_rows(...RowsMother::sortedGroupBatches()))->write(to_transformation($cacheRows, $spy))->run();

        static::assertCount(6, $spy->loadedRowsToArray());
        static::assertSame(6, df()->read(from_cache('transformation-blocking-operations'))->count());
    }

    public function test_collect_inside_a_transformation_collects_the_whole_stream(): void
    {
        // One load of 6 rows. This is the buffering cost a blocking operation has: collect() inside a Transformation
        // holds the stream in memory exactly as it does on an outer frame.
        $spy = new SpyLoader();
        $collectRows = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect());

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation($collectRows, $spy))
            ->run();

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame(1, $spy->loadsCount);
        static::assertSame([5, 4, 3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_constrain_inside_a_transformation_accumulates_across_batches(): void
    {
        // ConstrainedProcessor keeps its rowIndex and UniqueConstraint keeps its storage for the whole stream, so a
        // stream-wide constraint sees the whole stream. Each batch here is internally unique on 'g' while the stream
        // is not, and the violation is reported at row index 2, the first row of batch TWO.
        $uniqueGroup =
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->constrain(constraint_unique('g')));

        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('in row: 2');

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($uniqueGroup, new SpyLoader()))
            ->run();
    }

    public function test_drop_duplicates_inside_a_transformation_deduplicates_globally(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. DropDuplicatesTransformer is a Transformer holding its
        // hashes, and the side pipeline keeps it alive for the whole run.
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

    public function test_group_by_aggregate_inside_a_transformation_merges_groups_across_batches(): void
    {
        // Groups are merged across batches, giving the 2 rows the outer frame gives.
        $spy = new SpyLoader();
        $sumVByGroup = new CallbackTransformation(static function (DataFrame $df): DataFrame {
            return $df->groupBy([ref('g')])->aggregate(sum(ref('v')));
        });

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($sumVByGroup, $spy))
            ->run();

        static::assertSame([1, 1], $spy->loadedRowCounts());
        static::assertSame([['g' => 'a', 'v_sum' => 6.0], ['g' => 'b', 'v_sum' => 60.0]], $spy->loadedRowsToArray());
    }

    public function test_join_inside_a_transformation_matches_every_row(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. HashJoinProcessor buffers the RIGHT side, which is a separate
        // complete frame, and buckets the left side. Under one stream the buckets span the stream, so the output is
        // grouped by join key in 2 chunks of 3 - byte for byte what the same join gives on the outer frame.
        $spy = new SpyLoader();
        $joinNames = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->join(
            data_frame()->process(rows(
                schema(str_schema('code'), str_schema('n')),
                row(['code' => 'a', 'n' => 'Alpha']),
                row(['code' => 'b', 'n' => 'Bravo']),
            )),
            join_on(['g' => 'code'], 'j_'),
            Join::inner,
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($joinNames, $spy))
            ->run();

        static::assertSame([3, 3], $spy->loadedRowCounts());
        static::assertSame(
            [
                ['g' => 'a', 'v' => 1, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'a', 'v' => 2, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'a', 'v' => 3, 'j_code' => 'a', 'j_n' => 'Alpha'],
                ['g' => 'b', 'v' => 10, 'j_code' => 'b', 'j_n' => 'Bravo'],
                ['g' => 'b', 'v' => 20, 'j_code' => 'b', 'j_n' => 'Bravo'],
                ['g' => 'b', 'v' => 30, 'j_code' => 'b', 'j_n' => 'Bravo'],
            ],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_limit_inside_a_transformation_applies_across_the_whole_stream(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. LimitReachedException round-trips into Signal::STOP, which
        // ends the fed source and terminates the fiber, so the batches fed afterwards are dropped. Identical to
        // limiting the outer frame.
        $spy = new SpyLoader();
        $limitToThree = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->limit(3));

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation($limitToThree, $spy))
            ->run();

        static::assertSame([5, 4, 3], array_column($spy->loadedRowsToArray(), 'id'));
        static::assertSame([2, 1], $spy->loadedRowCounts());
        // The terminated fiber skips the drain, but closure() is still forwarded, so a file loader never orphans
        // its temporary file on a limited run.
        static::assertSame(1, $spy->closureCount);
    }

    public function test_offset_inside_a_transformation_skips_across_batches(): void
    {
        // The offset is consumed once, over the stream, instead of inside every batch.
        $spy = new SpyLoader();
        $skipTwo = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->offset(2));

        df()->read(from_rows(...RowsMother::descendingIdBatches()))->write(to_transformation($skipTwo, $spy))->run();

        static::assertSame([2, 2], $spy->loadedRowCounts());
        static::assertSame([3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_repartition_inside_a_transformation_regroups_the_whole_stream(): void
    {
        // Groups are cut over the stream, so each key arrives as one chunk instead of one per
        // incoming batch.
        $spy = new SpyLoader();
        $partitionByGroup = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->repartition(ref(
            'g',
        )));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($partitionByGroup, $spy))
            ->run();

        static::assertSame([3, 3], $spy->loadedRowCounts());
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

    public function test_pivot_inside_a_transformation_pivots_the_whole_stream(): void
    {
        // One pivoted set for the stream. Every row carries both pivoted columns in Schema order,
        // the group's own value and null for the other.
        $spy = new SpyLoader();
        $pivotGroupSums = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df
            ->groupBy([ref('g')])
            ->pivot(ref('g'), pivot_values('a', 'b'))
            ->aggregate(sum(ref('v'))));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($pivotGroupSums, $spy))
            ->run();

        static::assertSame([2], $spy->loadedRowCounts());
        static::assertSame(
            [['g' => 'a', 'a' => 6.0, 'b' => null], ['g' => 'b', 'a' => null, 'b' => 60.0]],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_row_number_inside_a_transformation_numbers_the_whole_stream(): void
    {
        // An unpartitioned window covers the stream, so the numbering runs 1..6 over the ordered rows.
        $spy = new SpyLoader();
        $numberByValue = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withEntry(
            'rn',
            row_number()->over(window()->orderBy(ref('v'))),
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($numberByValue, $spy))
            ->run();

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame(
            [
                ['g' => 'a', 'v' => 1, 'rn' => 1],
                ['g' => 'a', 'v' => 2, 'rn' => 2],
                ['g' => 'a', 'v' => 3, 'rn' => 3],
                ['g' => 'b', 'v' => 10, 'rn' => 4],
                ['g' => 'b', 'v' => 20, 'rn' => 5],
                ['g' => 'b', 'v' => 30, 'rn' => 6],
            ],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_sort_by_inside_a_transformation_sorts_the_whole_stream(): void
    {
        // The stream is sorted, not each batch on its own.
        $spy = new SpyLoader();
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')]));

        df()->read(from_rows(...RowsMother::descendingIdBatches()))->write(to_transformation($sortById, $spy))->run();

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_the_transformation_is_expanded_once_per_run(): void
    {
        // The Transformation is expanded once, when write() builds the plan, so one side pipeline serves the run.
        $expansions = 0;
        $spy = new SpyLoader();
        $reBatch = new CallbackTransformation(static function (DataFrame $df) use (&$expansions): DataFrame {
            $expansions++;

            return $df->batchSize(2);
        });

        df()->read(from_rows(...RowsMother::descendingIdBatches()))->write(to_transformation($reBatch, $spy))->run();

        static::assertSame(1, $expansions);
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_until_inside_a_transformation_applies_across_the_whole_stream(): void
    {
        // REGRESSION GUARD - asserts CORRECT behaviour. UntilTransformer is a Transformer and its STOP ends the side
        // pipeline. Identical to running until() on the outer frame.
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

    public function test_window_partition_by_inside_a_transformation_covers_the_whole_stream(): void
    {
        // Each partition holds every row of its group across the stream, so the averages are the group averages.
        $spy = new SpyLoader();
        $averageOverGroup = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withEntry(
            'avg',
            average(ref('v'))->over(window()->partitionBy(ref('g'))),
        ));

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation($averageOverGroup, $spy))
            ->run();

        static::assertSame([3, 3], $spy->loadedRowCounts());
        static::assertSame(
            [
                ['g' => 'a', 'v' => 1, 'avg' => 2.0],
                ['g' => 'a', 'v' => 2, 'avg' => 2.0],
                ['g' => 'a', 'v' => 3, 'avg' => 2.0],
                ['g' => 'b', 'v' => 10, 'avg' => 20.0],
                ['g' => 'b', 'v' => 20, 'avg' => 20.0],
                ['g' => 'b', 'v' => 30, 'avg' => 20.0],
            ],
            $spy->loadedRowsToArray(),
        );
    }
}
