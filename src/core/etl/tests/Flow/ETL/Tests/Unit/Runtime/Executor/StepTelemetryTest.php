<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Runtime\Executor\StepTelemetry;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Telemetry\Tracer\Span;
use RuntimeException;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_stream;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;
use function iterator_to_array;

final class StepTelemetryTest extends FlowTestCase
{
    public function test_a_transformation_span_carries_the_row_counts_and_the_step_attributes(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        (new StepTelemetry())->transform(
            new ScalarFunctionTransformer('b', lit(1)),
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a'))),
            $telemetry->flowContext,
        );

        $attributes = $telemetry->spans->endedSpans()[0]->attributes();
        static::assertSame(2, $attributes[TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS]);
        static::assertSame(2, $attributes[TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS]);
        static::assertSame(lit(1)::class, $attributes[TelemetryAttributes::ATTR_SCALAR_FUNCTION]);
    }

    public function test_a_reached_limit_completes_its_span_with_the_trimmed_row_count(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        static::assertCount(1, (new StepTelemetry())->transform(
            new LimitTransformer(1),
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a'))),
            $telemetry->flowContext,
        ));

        $span = $telemetry->spans->endedSpans()[0];
        static::assertFalse($span->status()?->isError() ?? false);
        static::assertSame(2, $span->attributes()[TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS]);
        static::assertSame(1, $span->attributes()[TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS]);
    }

    public function test_a_failing_transformation_ends_its_span_as_an_error_and_reaches_the_caller(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        $this->expectExceptionObject(new RuntimeException('refused'));

        try {
            (new StepTelemetry())->transform(
                new ThrowingTransformer(new RuntimeException('refused')),
                array_to_rows([['a' => 1]], schema(int_schema('a'))),
                $telemetry->flowContext,
            );
        } finally {
            static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError() ?? false);
        }
    }

    public function test_a_loading_span_carries_the_destination_of_a_file_loader(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        (new StepTelemetry())->load(
            to_floe(path('memory://out/file.floe'), filesystem: memory_filesystem()),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            $telemetry->flowContext,
        );

        $attributes = $telemetry->spans->endedSpans()[0]->attributes();
        static::assertSame('memory://out/file.floe', $attributes[TelemetryAttributes::ATTR_LOADER_DESTINATION_URI]);
        static::assertSame(1, $attributes[TelemetryAttributes::ATTR_LOADING_ROWS]);
    }

    public function test_a_loading_span_carries_the_loader_attributes(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        (new StepTelemetry())->load(
            to_stream('php://memory'),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            $telemetry->flowContext,
        );

        static::assertSame(
            'php://memory',
            $telemetry->spans->endedSpans()[0]->attributes()[TelemetryAttributes::ATTR_LOADER_DESTINATION_URI],
        );
    }

    public function test_an_untraced_transformation_opens_no_span(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: false));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        (new StepTelemetry())->transform(
            new ScalarFunctionTransformer('b', lit(1)),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            $telemetry->flowContext,
        );

        static::assertSame(
            ['DataFrame flow_dataframe'],
            array_map(static fn(Span $span): string => $span->name(), $telemetry->spans->startedSpans()),
        );
    }

    public function test_an_untraced_loading_opens_no_span(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: false));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        (new StepTelemetry())->load(
            to_stream('php://memory'),
            array_to_rows([['a' => 1]], schema(int_schema('a'))),
            $telemetry->flowContext,
        );

        static::assertSame(
            ['DataFrame flow_dataframe'],
            array_map(static fn(Span $span): string => $span->name(), $telemetry->spans->startedSpans()),
        );
    }

    public function test_an_untraced_operator_is_its_own_generator(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: false));
        $batches = GroupByContext::batches(array_to_rows([['a' => 1]], schema(int_schema('a'))));

        static::assertSame($batches, (new StepTelemetry())->operator(
            DistinctProcessor::class,
            $batches,
            $telemetry->flowContext,
        ));
    }

    public function test_a_traced_operator_yields_its_batches_and_returns_its_return_inside_one_span(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);
        $first = array_to_rows([['a' => 1]], schema(int_schema('a')));
        $second = array_to_rows([['a' => 2]], schema(int_schema('a')));

        $traced = (new StepTelemetry())->operator(
            DistinctProcessor::class,
            (static function () use ($first, $second) {
                yield $first;
                yield $second;

                return 'done';
            })(),
            $telemetry->flowContext,
        );

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertSame([$first, $second], iterator_to_array($traced, false));
        static::assertSame('done', $traced->getReturn());
        static::assertSame(
            ['DistinctProcessor'],
            array_map(static fn(Span $span): string => $span->name(), $telemetry->spans->endedSpans()),
        );
        static::assertNull($telemetry->spans->endedSpans()[0]->status());
    }

    public function test_a_traced_operator_forwards_a_stop_to_the_operator(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);
        $received = new ArrayObject();

        $traced = (new StepTelemetry())->operator(
            DistinctProcessor::class,
            (static function () use ($received) {
                $received[] = yield array_to_rows([['a' => 1]], schema(int_schema('a')));
            })(),
            $telemetry->flowContext,
        );
        $traced->current();
        $traced->send(Signal::STOP);

        static::assertSame([Signal::STOP], $received->getArrayCopy());
    }

    public function test_a_failing_operator_ends_its_span_as_an_error_and_reaches_the_caller(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        $this->expectExceptionObject(new RuntimeException('refused'));

        try {
            iterator_to_array((new StepTelemetry())->operator(
                DistinctProcessor::class,
                (static function () {
                    yield array_to_rows([['a' => 1]], schema(int_schema('a')));

                    throw new RuntimeException('refused');
                })(),
                $telemetry->flowContext,
            ));
        } finally {
            static::assertSame('refused', $telemetry->spans->endedSpans()[0]->status()?->description);
            static::assertCount(1, $telemetry->logs->entriesContaining('Operator failed'));
        }
    }

    public function test_an_abandoned_operator_completes_its_span_without_an_error(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        $traced = (new StepTelemetry())->operator(
            DistinctProcessor::class,
            GroupByContext::batches(
                array_to_rows([['a' => 1]], schema(int_schema('a'))),
                array_to_rows([['a' => 2]], schema(int_schema('a'))),
            ),
            $telemetry->flowContext,
        );
        $traced->current();
        unset($traced);

        static::assertSame(
            ['DistinctProcessor'],
            array_map(static fn(Span $span): string => $span->name(), $telemetry->spans->endedSpans()),
        );
        static::assertNull($telemetry->spans->endedSpans()[0]->status());
    }

    public function test_a_failing_loader_ends_its_span_as_an_error_and_reaches_the_caller(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(trace_loading: true));
        $telemetry->flowContext->telemetry()->dataFrameStarted([]);

        $this->expectExceptionObject(new RuntimeException('refused'));

        try {
            (new StepTelemetry())->load(
                new ThrowingLoader(new RuntimeException('refused')),
                array_to_rows([['a' => 1]], schema(int_schema('a'))),
                $telemetry->flowContext,
            );
        } finally {
            static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError() ?? false);
        }
    }
}
