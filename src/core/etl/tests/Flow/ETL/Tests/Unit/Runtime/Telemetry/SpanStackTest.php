<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Telemetry;

use Flow\ETL\Runtime\Telemetry\SpanScope;
use Flow\ETL\Runtime\Telemetry\SpanStack;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Telemetry\Tracer\Span;

use function array_map;

final class SpanStackTest extends FlowTestCase
{
    public function test_drain_completes_every_span_with_error_status(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $outer = $tracer->span('outer');
        $stack->push(new SpanScope($outer, $tracer->activate($outer)));
        $inner = $tracer->span('inner');
        $stack->push(new SpanScope($inner, $tracer->activate($inner)));

        $stack->drain($tracer, 'Span was never completed.');

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, $endedSpans);

        foreach ($endedSpans as $span) {
            $status = $span->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame('Span was never completed.', $status->description);
        }
    }

    public function test_drain_completes_spans_in_last_in_first_out_order(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $outer = $tracer->span('outer');
        $stack->push(new SpanScope($outer, $tracer->activate($outer)));
        $inner = $tracer->span('inner');
        $stack->push(new SpanScope($inner, $tracer->activate($inner)));

        $stack->drain($tracer, 'Span was never completed.');

        static::assertSame(
            ['inner', 'outer'],
            array_map(static fn(Span $span): string => $span->name(), $context->spans->endedSpans()),
        );
    }

    public function test_drain_empties_the_stack(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $outer = $tracer->span('outer');
        $spanScope = new SpanScope($outer, $tracer->activate($outer));
        $stack->push($spanScope);

        $stack->drain($tracer, 'Span was never completed.');

        static::assertFalse($stack->remove($spanScope));
    }

    public function test_drain_on_empty_stack_completes_nothing(): void
    {
        $context = new MemoryTelemetryContext();

        (new SpanStack())->drain($context->telemetry->tracer('flow-php'), 'Span was never completed.');

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_drain_completes_a_span_that_has_no_scope(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $stack->push(new SpanScope($tracer->span('operator'), null));

        $stack->drain($tracer, 'Span was never completed.');

        static::assertSame(
            ['operator'],
            array_map(static fn(Span $span): string => $span->name(), $context->spans->endedSpans()),
        );
    }

    public function test_drain_detaches_every_scope(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $outer = $tracer->span('outer');
        $stack->push(new SpanScope($outer, $tracer->activate($outer)));
        $inner = $tracer->span('inner');
        $stack->push(new SpanScope($inner, $tracer->activate($inner)));

        $stack->drain($tracer, 'Span was never completed.');

        static::assertNull($tracer->activeSpan());
    }

    public function test_remove_of_a_span_never_pushed_returns_false(): void
    {
        $tracer = (new MemoryTelemetryContext())->telemetry->tracer('flow-php');

        static::assertFalse((new SpanStack())->remove(new SpanScope($tracer->span('outer'), null)));
    }

    public function test_remove_takes_out_that_span_only_whatever_its_position(): void
    {
        $context = new MemoryTelemetryContext();
        $tracer = $context->telemetry->tracer('flow-php');

        $stack = new SpanStack();
        $outer = new SpanScope($tracer->span('outer'), null);
        $stack->push($outer);
        $stack->push(new SpanScope($tracer->span('inner'), null));

        static::assertTrue($stack->remove($outer));
        static::assertFalse($stack->remove($outer));

        $stack->drain($tracer, 'Span was never completed.');

        static::assertSame(
            ['inner'],
            array_map(static fn(Span $span): string => $span->name(), $context->spans->endedSpans()),
        );
    }
}
