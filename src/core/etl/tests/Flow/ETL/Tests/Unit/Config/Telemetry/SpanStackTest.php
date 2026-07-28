<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use Flow\ETL\Config\Telemetry\SpanStack;
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
        $stack->push($tracer->span('outer'));
        $stack->push($tracer->span('inner'));

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
        $stack->push($tracer->span('outer'));
        $stack->push($tracer->span('inner'));

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
        $stack->push($tracer->span('outer'));

        $stack->drain($tracer, 'Span was never completed.');

        static::assertNull($stack->pop());
    }

    public function test_drain_on_empty_stack_completes_nothing(): void
    {
        $context = new MemoryTelemetryContext();

        (new SpanStack())->drain($context->telemetry->tracer('flow-php'), 'Span was never completed.');

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_pop_returns_null_when_stack_is_empty(): void
    {
        static::assertNull((new SpanStack())->pop());
    }

    public function test_pop_returns_spans_in_last_in_first_out_order(): void
    {
        $tracer = (new MemoryTelemetryContext())->telemetry->tracer('flow-php');

        $outer = $tracer->span('outer');
        $inner = $tracer->span('inner');

        $stack = new SpanStack();
        $stack->push($outer);
        $stack->push($inner);

        static::assertSame($inner, $stack->pop());
        static::assertSame($outer, $stack->pop());
        static::assertNull($stack->pop());
    }
}
