<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
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
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_schema;
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
        $loader = to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->with(
            new ThrowWhenRowMatches('id', 1, new RuntimeException('boom')),
        )), $spy);

        $thrown = null;

        try {
            $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertSame('boom', $thrown->getMessage());

        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $context);
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
        $loader = to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->with(
            new ThrowWhenRowMatches('id', 1, new RuntimeException('boom')),
        )), $spy);

        $thrown = null;

        try {
            $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $failed);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertSame('boom', $thrown->getMessage());

        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $next);
        $loader->closure($next);

        static::assertSame(
            [[['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
        static::assertSame([$next], $spy->contexts);
    }

    public function test_a_batch_from_a_new_run_does_not_reuse_a_suspended_drive(): void
    {
        $first = flow_context(config());
        $second = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(add_row_index('n', StartFrom::ONE), $spy);

        // Run 1 dies via a sibling step, so closure() never runs and the stream is left suspended, not dropped.
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $first);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $second);

        static::assertSame(
            [[['id' => 1, 'n' => 1]], [['id' => 2, 'n' => 1]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $spy->loadedRows),
        );
        static::assertSame([$first, $second], $spy->contexts);
    }

    public function test_a_terminated_drive_skips_later_batches_and_still_closes_the_wrapped_loader(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->limit(2),
        ), $spy);

        for ($id = 1; $id <= 4; $id++) {
            $loader->load(rows(schema(int_schema('id')), row(['id' => $id])), $context);
        }

        $loader->closure($context);

        static::assertSame(2, $spy->loadsCount);
        static::assertSame([1, 1], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_closure_for_a_new_run_does_not_drain_a_dead_runs_drive(): void
    {
        $dead = flow_context(config());
        $next = flow_context(config());
        $spy = new SpyLoader();
        $loader = to_transformation(new CallbackTransformation(
            static fn(DataFrame $df): DataFrame => $df->collect(),
        ), $spy);

        // Run 1 buffers a batch in the stream and dies without closure(); run 2 routes no batches to this loader.
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $dead);
        $loader->closure($next);

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    public function test_closure_after_a_declined_drain_failure_closes_the_wrapped_loader(): void
    {
        $context = flow_context(config())->setErrorHandler(ignore_error_handler());
        $spy = new SpyLoader();
        $loader = to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->with(new ThrowingTransformer(
                new RuntimeException('boom'),
            ))),
            $spy,
        );

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $context);

        static::assertSame(0, $throwing->loadsCount);

        $thrown = null;

        try {
            $loader->closure($context);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame($failure, $thrown);

        static::assertSame(1, $throwing->loadsCount);

        $loader->load(rows(schema(int_schema('id')), row(['id' => 3])), $context);

        $thrown = null;

        try {
            $loader->closure($context);
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame($failure, $thrown);

        static::assertSame(2, $throwing->loadsCount);
    }

    public function test_a_failed_drive_is_rebuilt_for_the_next_batch(): void
    {
        // The dead fiber is dropped, so the loader stays usable: the batch offered after a failure reaches a fresh
        // stream and the destination again. Without this a RetryLoader could never re-offer a batch.
        $context = flow_context(config());
        $throwing = new ThrowingLoader($failure = new RuntimeException('boom'));
        $loader = to_transformation(select('id'), $throwing);

        foreach ([1, 2] as $id) {
            $thrown = null;

            try {
                $loader->load(rows(schema(int_schema('id')), row(['id' => $id])), $context);
            } catch (RuntimeException $e) {
                $thrown = $e;
            }

            static::assertSame($failure, $thrown);
        }

        static::assertSame(2, $throwing->loadsCount);
    }

    public function test_a_declined_failure_keeps_loading_into_the_next_loader_like_a_plain_loader(): void
    {
        // skipLoader declines one sink's failure, not the batch: the loader after this one still receives it -
        // exactly what a plain loader's declined failure does.
        $tail = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->onError(ignore_error_handler())
            ->write(to_transformation(new ThrowWhenRowMatches('id', 2, new RuntimeException('boom')), new SpyLoader()))
            ->write($tail)
            ->run();

        static::assertSame([1, 2, 3], array_column($tail->loadedRowsToArray(), 'id'));
    }

    public function test_closure_reports_a_drain_failure_as_a_loading_error(): void
    {
        $handler = new RecordingErrorHandler();
        $context = flow_context(config())->setErrorHandler($handler);
        $loader = to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->with(new ThrowingTransformer(
                new RuntimeException('boom'),
            ))),
            new SpyLoader(),
        );

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->closure($context);

        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(LoadingError::class, $handler->errors[0]);
        static::assertSame($loader, $handler->errors[0]->loader);
        static::assertSame('boom', $handler->errors[0]->cause->getMessage());
    }

    public function test_closure_rethrows_a_drain_failure_under_skip_rows(): void
    {
        $context = flow_context(config())->setErrorHandler(skip_rows_handler());
        $loader = to_transformation(
            new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->collect()->with(new ThrowingTransformer(
                new RuntimeException('boom'),
            ))),
            new SpyLoader(),
        );

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('boom');

        $loader->closure($context);
    }

    public function test_a_failed_run_does_not_close_the_wrapped_loader(): void
    {
        // Req 3, pinned where it actually holds: Segment propagates the failure before reaching its closure loop
        // (Segment.php:98 vs :119), so no loader in the segment is closed.
        $spy = new SpyLoader();

        $thrown = null;

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->write(to_transformation(new ThrowingTransformer(new RuntimeException('boom')), $spy))
                ->run();
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertSame('boom', $thrown->getMessage());

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
            $loader->load(rows(schema(int_schema('id')), row(['id' => $id])), $context);
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

    public function test_a_rebuilt_drive_does_not_re_report_the_same_runs_limit(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $loader = to_transformation(select('id'), new ThrowingLoader(new LimitReachedException(1)));

        // The first load drops the stream; the second arrives on the SAME run, rebuilds it, and the sink throws again.
        // One logical limit event, so exactly one report.
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $telemetry->flowContext);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $telemetry->flowContext);

        static::assertCount(1, $telemetry->logs->entriesContaining('Limit reached'));
        static::assertEmpty($telemetry->logs->entriesContaining('Loading failed'));
    }

    public function test_a_second_run_reports_its_own_limit(): void
    {
        $first = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $second = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $loader = to_transformation(select('id'), new ThrowingLoader(new LimitReachedException(1)));

        // Run 1 dies without closure(), so only the run-change check can re-arm reporting for run 2.
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $first->flowContext);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $second->flowContext);

        static::assertCount(1, $first->logs->entriesContaining('Limit reached'));
        static::assertCount(1, $second->logs->entriesContaining('Limit reached'));
        static::assertEmpty($second->logs->entriesContaining('Loading failed'));
    }

    public function test_a_transformer_loader_is_never_replay_safe(): void
    {
        static::assertFalse(to_transformation(select('id'), new SpyLoader())->replaySafe());
        static::assertFalse(to_transformation(new LimitTransformer(1), new SpyLoader())->replaySafe());
    }

    public function test_limit_reached_is_reported_once_per_loader(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $loader = to_transformation(new LimitTransformer(1), to_memory(new ArrayMemory()));
        $batch = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));

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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $first->flowContext);
        $loader->closure($first->flowContext);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $second->flowContext);

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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 2])), $context);
        $loader->closure($context);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 3])), $context);

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
            $loader->load(rows(schema(int_schema('id')), row(['id' => $id])), $context);
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
            $loader->load(rows(schema(int_schema('id')), row(['id' => $id])), $context);
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
            $loader->load(
                rows(schema(int_schema('id'), str_schema('name')), row(['id' => $id, 'name' => 'name-' . $id])),
                $context,
            );
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
        $transformerMock->expects(self::once())->method('transform')->willReturn(rows(schema()));

        $loaderMock = $this->createMock(Loader::class);
        $loaderMock->expects(self::once())->method('load');

        $transformer = to_transformation($transformerMock, $loaderMock);

        $transformer->load(rows(schema()), flow_context(config()));
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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);

        static::assertSame([$context], $spy->contexts);
    }
}
