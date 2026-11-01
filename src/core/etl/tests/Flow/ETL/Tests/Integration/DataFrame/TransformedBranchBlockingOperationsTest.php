<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Exception\ConstraintViolationException;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Join\JoinType;
use RuntimeException;

use function array_column;
use function Flow\ETL\DSL\array_to_rows;
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
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\window;

final class TransformedBranchBlockingOperationsTest extends FlowIntegrationTestCase
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

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->aggregate([sum(ref('v'))])),
                $spy,
            ))
            ->run();

        static::assertSame([['v_sum' => 66.0]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_batch_by_inside_a_transformation_batches_by_group_across_the_stream(): void
    {
        // Chunks are cut at the group boundaries of the whole stream, not of each source batch.
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::sortedGroupBatches()))
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->batchBy(ref('g'))),
                $spy,
            ))
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

        df()
            ->read(from_rows(...RowsMother::sortedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->cache('transformation-blocking-operations'),
            ), $spy))
            ->run();

        static::assertCount(6, $spy->loadedRowsToArray());
        static::assertSame(6, df()->read(from_cache('transformation-blocking-operations'))->count());
    }

    public function test_collect_inside_a_transformation_collects_the_whole_stream(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->collect(),
            ), $spy))
            ->run();

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame(1, $spy->loadsCount);
        static::assertSame([5, 4, 3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_constrain_inside_a_transformation_accumulates_across_batches(): void
    {
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
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static function (DataFrame $df): DataFrame {
                return $df->dropDuplicates(ref('g'));
            }), $spy))
            ->run();

        static::assertSame([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 10]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_group_by_aggregate_inside_a_transformation_merges_groups_across_batches(): void
    {
        // Groups are merged across batches, giving the 2 rows the outer frame gives - in one batch, aggregated in memory.
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static function (DataFrame $df): DataFrame {
                return $df->groupBy([ref('g')])->aggregate(sum(ref('v')));
            }), $spy))
            ->run();

        static::assertSame([2], $spy->loadedRowCounts());
        static::assertSame([['g' => 'a', 'v_sum' => 6.0], ['g' => 'b', 'v_sum' => 60.0]], $spy->loadedRowsToArray());
    }

    public function test_join_inside_a_transformation_matches_every_row(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->join(
                data_frame()->process(array_to_rows(
                    [['code' => 'a', 'n' => 'Alpha'], ['code' => 'b', 'n' => 'Bravo']],
                    schema(str_schema('code'), str_schema('n')),
                )),
                join_on(['g' => 'code'], 'j_'),
                JoinType::inner,
            )), $spy))
            ->run();

        static::assertSame([2, 2, 2], $spy->loadedRowCounts());
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
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->limit(3),
            ), $spy))
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

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->offset(2),
            ), $spy))
            ->run();

        static::assertSame([2, 2], $spy->loadedRowCounts());
        static::assertSame([3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_repartition_inside_a_transformation_regroups_the_whole_stream(): void
    {
        // Groups are cut over the stream, so each key arrives as one chunk instead of one per
        // incoming batch.
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->repartition(ref('g'))),
                $spy,
            ))
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
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df
                ->groupBy([ref('g')])
                ->pivot(ref('g'), pivot_values('a', 'b'))
                ->aggregate(sum(ref('v')))), $spy))
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

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withColumn(
                'rn',
                row_number()->over(window()->orderBy(ref('v'))),
            )), $spy))
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

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                $spy,
            ))
            ->run();

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_the_transformation_is_expanded_once_per_run(): void
    {
        // The Transformation is expanded once, when write() builds the plan, so one branch pipeline serves the run.
        $expansions = 0;
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_transformation(new CallbackTransformation(static function (DataFrame $df) use (
                &$expansions,
            ): DataFrame {
                $expansions++;

                return $df->batchSize(2);
            }), $spy))
            ->run();

        static::assertSame(1, $expansions);
        static::assertSame(3, $spy->loadsCount);
    }

    public function test_until_inside_a_transformation_applies_across_the_whole_stream(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static function (DataFrame $df): DataFrame {
                return $df->until(ref('v')->lessThan(lit(3)));
            }), $spy))
            ->run();

        static::assertSame([['g' => 'a', 'v' => 1]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_window_partition_by_inside_a_transformation_covers_the_whole_stream(): void
    {
        // Each partition holds every row of its group across the stream, so the averages are the group averages.
        $spy = new SpyLoader();

        df()
            ->read(from_rows(...RowsMother::interleavedGroupBatches()))
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->withColumn(
                'avg',
                average(ref('v'))->over(window()->partitionBy(ref('g'))),
            )), $spy))
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
