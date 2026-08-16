<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\Telemetry\Tracer\Span;
use RuntimeException;

use function array_column;
use function array_filter;
use function array_map;
use function count;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Types\DSL\type_string;

final class TransformerLoaderTest extends FlowTestCase
{
    public function test_a_declined_error_handler_starts_a_fresh_drive_for_the_next_batch(): void
    {
        $context = flow_context(config())->setErrorHandler(ignore_error_handler());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->map(
            static fn(Row $row): Row => $row->valueOf('id') === 1 ? throw new RuntimeException('boom') : $row,
        )), $spy);

        try {
            $loader->load(rows(row(int_entry('id', 1))), $context);

            static::fail('Expected the failure to reach the caller even though the handler declined it.');
        } catch (RuntimeException $e) {
            static::assertSame('boom', $e->getMessage());
        }

        $loader->load(rows(row(int_entry('id', 2))), $context);
        $loader->closure($context);

        static::assertSame(
            [[['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_new_flow_context_starts_a_fresh_drive_after_a_failed_run(): void
    {
        $failed = flow_context(config());
        $next = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->map(
            static fn(Row $row): Row => $row->valueOf('id') === 1 ? throw new RuntimeException('boom') : $row,
        )), $spy);

        try {
            $loader->load(rows(row(int_entry('id', 1))), $failed);

            static::fail('Expected the nested map() to propagate.');
        } catch (RuntimeException $e) {
            static::assertSame('boom', $e->getMessage());
        }

        $loader->load(rows(row(int_entry('id', 2))), $next);
        $loader->closure($next);

        static::assertSame(
            [[['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
        static::assertSame([$next], $spy->contexts);
    }

    public function test_a_terminated_drive_skips_later_batches_and_still_closes_the_wrapped_loader(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->limit(2),
        ), $spy);

        for ($id = 1; $id <= 4; $id++) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        $loader->closure($context);

        static::assertSame(2, $spy->loadsCount);
        static::assertSame([1, 1], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_closure_after_a_declined_drain_failure_closes_the_wrapped_loader(): void
    {
        $context = flow_context(config())->setErrorHandler(ignore_error_handler());
        $spy = new SpyLoader();
        $loader = to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->map(
                static fn(Row $row): Row => throw new RuntimeException('boom'),
            )),
            $spy,
        );

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->closure($context);

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_drain_time_failure_rethrows_from_closure(): void
    {
        $context = flow_context(config());
        $throwing = new ThrowingLoader($failure = new RuntimeException('boom'));
        $loader = to_transformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->collect(),
        ), $throwing);

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->load(rows(row(int_entry('id', 2))), $context);

        static::assertSame(0, $throwing->loadsCount);

        try {
            $loader->closure($context);

            static::fail('Expected the drain to rethrow the wrapped loader failure.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(1, $throwing->loadsCount);

        $loader->load(rows(row(int_entry('id', 3))), $context);

        try {
            $loader->closure($context);

            static::fail('Expected the second drain to rethrow the wrapped loader failure.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(2, $throwing->loadsCount);
    }

    public function test_a_failed_drive_is_rebuilt_for_the_next_batch(): void
    {
        // The dead fiber is dropped, so the loader stays usable: the batch offered after a failure reaches a fresh
        // drive and the destination again. Without this a RetryLoader could never re-offer a batch.
        $context = flow_context(config());
        $throwing = new ThrowingLoader($failure = new RuntimeException('boom'));
        $loader = to_transformation(select('id'), $throwing);

        foreach ([1, 2] as $id) {
            try {
                $loader->load(rows(row(int_entry('id', $id))), $context);

                static::fail('Expected the wrapped loader failure to propagate.');
            } catch (RuntimeException $e) {
                static::assertSame($failure, $e);
            }
        }

        static::assertSame(2, $throwing->loadsCount);
    }

    public function test_a_declined_failure_breaks_the_step_chain_like_a_plain_loader(): void
    {
        // skip_rows_handler() is the only handler under which the declined path is observable end to end: Segment
        // breaks the step chain for the failing batch, so the loader after this one never sees it - exactly what a
        // plain loader's failure does.
        $tail = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->onError(skip_rows_handler())
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->map(
                static fn(Row $row): Row => $row->valueOf('id') === 2 ? throw new RuntimeException('boom') : $row,
            )), new SpyLoader()))
            ->write($tail)
            ->run();

        static::assertSame([1, 3], array_column($tail->loadedRowsToArray(), 'id'));
    }

    public function test_a_failed_run_does_not_close_the_wrapped_loader(): void
    {
        // Req 3, pinned where it actually holds: Segment propagates the failure before reaching its closure loop
        // (Segment.php:98 vs :119), so no loader in the segment is closed.
        $spy = new SpyLoader();

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->map(
                    static fn(Row $row): Row => throw new RuntimeException('boom'),
                )), $spy))
                ->run();

            static::fail('Expected the nested map() to fail the run.');
        } catch (RuntimeException $e) {
            static::assertSame('boom', $e->getMessage());
        }

        static::assertSame(0, $spy->closureCount);
        static::assertSame(0, $spy->loadsCount);
    }

    public function test_closure_drains_the_output_a_blocking_operation_buffered(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->collect(),
        ), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        static::assertSame(0, $spy->loadsCount);

        $loader->closure($context);

        static::assertSame(1, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
        static::assertSame(
            [[['id' => 1], ['id' => 2], ['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_limit_reached_is_reported_once_per_loader(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $loader = to_transformation(new LimitTransformer(1), to_memory(new ArrayMemory()));
        $batch = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        $loader->load($batch, $context->flowContext);
        $loader->load($batch, $context->flowContext);
        $loader->load($batch, $context->flowContext);

        static::assertCount(1, $context->logs->entriesContaining('Limit reached'));
        static::assertEmpty($context->logs->entriesContaining('Loading failed'));

        $endedSpans = $context->spans->endedSpans();

        // MemoryLoader runs only on the first call; the other two throw before reaching it.
        static::assertSame(
            ['MemoryLoader', 'TransformerLoader', 'TransformerLoader', 'TransformerLoader'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );

        foreach ($endedSpans as $span) {
            static::assertNull($span->status());
        }
    }

    public function test_closure_rebuilds_the_transformation_against_the_next_context(): void
    {
        $first = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $second = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $loader = to_transformation(select('id'), to_memory(new ArrayMemory()));

        $loader->load(rows(row(int_entry('id', 1))), $first->flowContext);
        $loader->closure($first->flowContext);
        $loader->load(rows(row(int_entry('id', 2))), $second->flowContext);

        $memoryLoaderSpans = static fn(MemoryTelemetryContext $context): int => count(array_filter(
            $context->spans->endedSpans(),
            static fn(Span $span): bool => $span->name() === 'MemoryLoader',
        ));

        static::assertSame(1, $memoryLoaderSpans($first));
        static::assertSame(1, $memoryLoaderSpans($second));
    }

    public function test_closure_resets_stateful_transformation_state(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(add_row_index('n', StartFrom::ONE), $spy);

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->load(rows(row(int_entry('id', 2))), $context);
        $loader->closure($context);
        $loader->load(rows(row(int_entry('id', 3))), $context);

        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 2]], [['id' => 3, 'n' => 1]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_stateful_loader_is_closed_once_across_batches(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(select('id'), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        $loader->closure($context);

        static::assertSame(3, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
        static::assertSame(
            [[['id' => 1]], [['id' => 2]], [['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_stateful_transformation_keeps_state_across_batches(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(add_row_index('n', StartFrom::ONE), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id))), $context);
        }

        static::assertSame(3, $spy->loadsCount);
        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 2]], [['id' => 3, 'n' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_stateless_transformation_applies_to_every_batch(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(select('id'), $spy);

        for ($id = 1; $id <= 3; $id++) {
            $loader->load(rows(row(int_entry('id', $id), str_entry('name', 'name-' . $id))), $context);
        }

        static::assertSame(3, $spy->loadsCount);
        static::assertSame(
            [[['id' => 1]], [['id' => 2]], [['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
    }

    public function test_transformer_loader(): void
    {
        $transformerMock = $this->createMock(Transformer::class);
        $transformerMock->expects(self::once())->method('transform')->willReturn(rows());

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects(self::once())->method('load');

        $transformer = to_transformation($transformerMock, $loaderMock);

        $transformer->load(rows(), flow_context(config()));
    }

    public function test_transformer_loader_with_transformation(): void
    {
        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->write(to_transformation(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('id_string', ref('id')->cast(type_string()));
                }
            }, to_memory($memory = new ArrayMemory())))
            ->run();

        static::assertEquals(
            [
                ['id' => 1, 'id_string' => '1'],
                ['id' => 2, 'id_string' => '2'],
                ['id' => 3, 'id_string' => '3'],
            ],
            $memory->dump(),
        );
    }

    public function test_wrapped_loader_receives_the_outer_flow_context(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(select('id'), $spy);

        $loader->load(rows(row(int_entry('id', 1))), $context);

        static::assertSame([$context], $spy->contexts);
    }
}
