<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use RuntimeException;

use function array_column;
use function array_map;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_branch;

final class BranchingLoaderTest extends FlowTestCase
{
    public function test_a_batch_from_a_new_run_does_not_reuse_a_suspended_drive(): void
    {
        $first = flow_context(config());
        $second = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_branch(lit(true), $spy)->withTransformation(add_row_index('n', StartFrom::ONE));

        $loader->load(rows(row(int_entry('id', 1))), $first);
        $loader->load(rows(row(int_entry('id', 2))), $second);

        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 1]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
        static::assertSame([$first, $second], $spy->contexts);
    }

    public function test_a_constructor_transformation_spans_the_whole_stream(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));
        $loader = to_branch(lit(true), $spy, $sortById);

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        $loader->closure($context);

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_a_drain_time_failure_rethrows_from_closure(): void
    {
        $failure = new RuntimeException('sink exploded');
        $sink = new ThrowingLoader($failure);
        $context = flow_context(config());
        $loader = to_branch(lit(true), $sink)->withTransformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->collect(),
        ));

        $loader->load(rows(row(int_entry('id', 1))), $context);

        static::assertSame(0, $sink->loadsCount);

        $thrown = null;

        try {
            $loader->closure($context);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame($failure, $thrown);

        static::assertSame(1, $sink->loadsCount);

        // The throwing closure() must still have reset the drive - the next round on the same context starts fresh.
        $loader->load(rows(row(int_entry('id', 2))), $context);

        $thrown = null;

        try {
            $loader->closure($context);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame($failure, $thrown);

        static::assertSame(2, $sink->loadsCount);
    }

    public function test_a_failed_drive_is_rebuilt_for_the_next_batch(): void
    {
        $failure = new RuntimeException('sink exploded');
        $sink = new ThrowingLoader($failure);
        $context = flow_context(config());
        $loader = to_branch(lit(true), $sink)->withTransformation(select('id'));

        foreach ([1, 2] as $id) {
            $thrown = null;

            try {
                $loader->load(rows(row(int_entry('id', $id))), $context);
            } catch (RuntimeException $e) {
                $thrown = $e;
            }

            static::assertSame($failure, $thrown);
        }

        static::assertSame(2, $sink->loadsCount);
    }

    public function test_a_fully_filtered_batch_produces_no_sink_calls(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(lit(false), $spy)->withTransformation(select('id'));

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->load(rows(row(int_entry('id', 2))), $context);
        $loader->closure($context);

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_rebuilt_drive_does_not_re_report_the_same_runs_limit(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $sink = new ThrowingLoader(new LimitReachedException(1));
        $loader = to_branch(lit(true), $sink)->withTransformation(select('id'));

        $loader->load(rows(row(int_entry('id', 1))), $telemetry->flowContext);
        $loader->load(rows(row(int_entry('id', 2))), $telemetry->flowContext);

        static::assertCount(1, $telemetry->logs->entriesContaining('Limit reached'));
        static::assertEmpty($telemetry->logs->entriesContaining('Loading failed'));
    }

    public function test_a_second_run_reports_its_own_limit(): void
    {
        $first = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $second = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $sink = new ThrowingLoader(new LimitReachedException(1));
        $loader = to_branch(lit(true), $sink)->withTransformation(select('id'));

        $loader->load(rows(row(int_entry('id', 1))), $first->flowContext);
        $loader->load(rows(row(int_entry('id', 2))), $second->flowContext);

        static::assertCount(1, $first->logs->entriesContaining('Limit reached'));
        static::assertCount(1, $second->logs->entriesContaining('Limit reached'));
        static::assertEmpty($second->logs->entriesContaining('Loading failed'));
    }

    public function test_a_streaming_transformation_delivers_per_batch(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(lit(true), $spy)->withTransformation(select('id'));

        $expected = 0;

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);

            static::assertSame(++$expected, $spy->loadsCount);
        }
    }

    public function test_a_terminated_drive_skips_later_batches_and_still_closes_the_wrapped_loader(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(lit(true), $spy)->withTransformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->limit(2),
        ));

        foreach ([1, 2, 3, 4] as $id) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        $loader->closure($context);

        static::assertSame(2, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_transformation_spans_the_whole_stream(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));
        $loader = to_branch(lit(true), $spy)->withTransformation($sortById);

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        $loader->closure($context);

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_a_triggering_transformation_is_refused(): void
    {
        $trigger = new CallbackTransformation(static function (DataFrame $df): DataFrame {
            $df->count();

            return $df;
        });
        $loader = to_branch(lit(true), new SpyLoader())->withTransformation($trigger);

        try {
            $loader->load(rows(row(int_entry('id', 1))), flow_context(config()));

            static::fail('Expected a Transformation triggering the nested frame to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('must only build the DataFrame', $e->getMessage());
        }
    }

    public function test_arming_a_transformation_mid_run_spans_only_the_remaining_batches(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(lit(true), $spy);
        $batches = RowsMother::descendingIdBatches();
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));

        $loader->load($batches[0], $context);
        $loader->withTransformation($sortById);
        $loader->load($batches[1], $context);
        $loader->load($batches[2], $context);
        $loader->closure($context);

        static::assertSame([2, 4], $spy->loadedRowCounts());
        static::assertSame([5, 4, 0, 1, 2, 3], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_closure_after_a_declined_drain_failure_closes_the_wrapped_loader(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config())->setErrorHandler(ignore_error_handler());
        $throwOnDrain = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->map(
            static fn(Row $row): Row => throw new RuntimeException('boom'),
        ));
        $loader = to_branch(lit(true), $spy)->withTransformation($throwOnDrain);

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->closure($context);

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_closure_for_a_new_run_does_not_drain_a_dead_runs_drive(): void
    {
        $dead = flow_context(config());
        $next = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_branch(lit(true), $spy)->withTransformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->collect(),
        ));

        // Run 1 buffers a batch in the drive and dies without closure(); run 2 routes no batches to this loader.
        $loader->load(rows(row(int_entry('id', 1))), $dead);
        $loader->closure($next);

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_closure_is_forwarded_to_the_wrapped_loader(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());

        to_branch(lit(true), $spy)->closure($context);

        static::assertSame(1, $spy->closureCount);
        static::assertSame([$context], $spy->closureContexts);
    }

    public function test_closure_resets_the_drive_for_the_next_run_on_the_same_context(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(lit(true), $spy)->withTransformation(add_row_index('n', StartFrom::ONE));

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->load(rows(row(int_entry('id', 2))), $context);
        $loader->closure($context);
        $loader->load(rows(row(int_entry('id', 3))), $context);

        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 2]], [['id' => 3, 'n' => 1]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_limit_reached_is_reported_once_per_loader(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $loader = to_branch(lit(true), new ThrowingLoader(new LimitReachedException(1)));
        $batch = rows(row(int_entry('id', 1)));

        $loader->load($batch, $telemetry->flowContext);
        $loader->load($batch, $telemetry->flowContext);
        $loader->load($batch, $telemetry->flowContext);

        static::assertCount(1, $telemetry->logs->entriesContaining('Limit reached'));
        static::assertEmpty($telemetry->logs->entriesContaining('Loading failed'));
    }

    public function test_loading_rows_counts_the_rows_offered_to_the_branch(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $loader = to_branch(ref('id')->greaterThanEqual(lit(2)), new SpyLoader());

        // Input 2 rows, 0 after the filter - the attribute counts the offer, not the post-filter delivery.
        $loader->load(RowsMother::descendingIdBatches()[2], $telemetry->flowContext);

        $spans = $telemetry->spans->endedSpans();

        static::assertCount(1, $spans);
        static::assertSame('BranchingLoader', $spans[0]->name());
        static::assertSame(2, $spans[0]->attributes()['flow.etl.loading.rows']);
    }

    public function test_replacing_the_transformation_mid_run_keeps_the_drive_built_first(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $batches = RowsMother::descendingIdBatches();
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));
        $loader = to_branch(lit(true), $spy)->withTransformation($sortById);

        $loader->load($batches[0], $context);
        $loader->withTransformation(select('id'));
        $loader->load($batches[1], $context);
        $loader->load($batches[2], $context);
        $loader->closure($context);

        static::assertSame([6], $spy->loadedRowCounts());
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_replay_safe_is_false_with_a_constructor_transformation(): void
    {
        static::assertFalse(to_branch(lit(true), new SpyLoader(), select('id'))->replaySafe());
    }

    public function test_replay_safe_only_without_a_transformation(): void
    {
        $loader = to_branch(lit(true), new SpyLoader());

        static::assertTrue($loader->replaySafe());

        $loader->withTransformation(select('id'));

        static::assertFalse($loader->replaySafe());
    }

    public function test_rows_matching_the_condition_reach_the_wrapped_loader(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(ref('id')->greaterThanEqual(lit(2)), $spy);

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        static::assertSame(3, $spy->loadsCount);
        static::assertSame([2, 2, 0], $spy->loadedRowCounts());
        static::assertSame([5, 4, 3, 2], array_column($spy->loadedRowsToArray(), 'id'));
    }

    public function test_the_condition_filters_before_the_transformation(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $loader = to_branch(ref('id')->greaterThanEqual(lit(2)), $spy);
        $loader->withTransformation(add_row_index('n', StartFrom::ONE));

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        $loader->closure($context);

        static::assertSame(
            [['id' => 5, 'n' => 1], ['id' => 4, 'n' => 2], ['id' => 3, 'n' => 3], ['id' => 2, 'n' => 4]],
            $spy->loadedRowsToArray(),
        );
    }

    public function test_the_nested_frame_is_built_once_per_loader(): void
    {
        $transformCalls = 0;
        $context = flow_context(config());
        $counting = new CallbackTransformation(static function (DataFrame $df) use (&$transformCalls): DataFrame {
            ++$transformCalls;

            return $df->select('id');
        });
        $loader = to_branch(lit(true), new SpyLoader())->withTransformation($counting);

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        static::assertSame(1, $transformCalls);
    }

    public function test_with_transformation_overrides_the_constructor_transformation(): void
    {
        $spy = new SpyLoader();
        $context = flow_context(config());
        $sortById = new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')));
        $loader = to_branch(lit(true), $spy, $sortById)->withTransformation(select('id'));

        foreach (RowsMother::descendingIdBatches() as $batch) {
            $loader->load($batch, $context);
        }

        $loader->closure($context);

        static::assertSame([2, 2, 2], $spy->loadedRowCounts());
        static::assertSame([5, 4, 3, 2, 1, 0], array_column($spy->loadedRowsToArray(), 'id'));
    }
}
