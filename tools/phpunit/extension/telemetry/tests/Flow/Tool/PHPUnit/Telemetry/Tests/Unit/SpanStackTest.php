<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Tests\Unit;

use function Flow\Telemetry\DSL\{span_id, trace_id};
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind};
use Flow\Tool\PHPUnit\Telemetry\SpanStack;
use PHPUnit\Framework\TestCase;

final class SpanStackTest extends TestCase
{
    public function test_clear_removes_all_spans() : void
    {
        $stack = new SpanStack();
        $span1 = $this->createSpan('span-1');
        $span2 = $this->createSpan('span-2');
        $suiteSpan = $this->createSpan('suite-span');

        $stack->push($span1);
        $stack->push($span2);
        $stack->setSuiteSpan('TestSuite', $suiteSpan);

        $stack->clear();

        self::assertTrue($stack->isEmpty());
        self::assertNull($stack->getSuiteSpan('TestSuite'));
    }

    public function test_current_returns_top_without_removing() : void
    {
        $stack = new SpanStack();
        $span = $this->createSpan('test-span');

        $stack->push($span);

        self::assertSame($span, $stack->current());
        self::assertSame($span, $stack->current());
        self::assertFalse($stack->isEmpty());
    }

    public function test_lifo_order() : void
    {
        $stack = new SpanStack();
        $span1 = $this->createSpan('span-1');
        $span2 = $this->createSpan('span-2');
        $span3 = $this->createSpan('span-3');

        $stack->push($span1);
        $stack->push($span2);
        $stack->push($span3);

        self::assertSame($span3, $stack->pop());
        self::assertSame($span2, $stack->pop());
        self::assertSame($span1, $stack->pop());
        self::assertTrue($stack->isEmpty());
    }

    public function test_push_and_pop() : void
    {
        $stack = new SpanStack();
        $span = $this->createSpan('test-span');

        $stack->push($span);

        self::assertFalse($stack->isEmpty());
        self::assertSame($span, $stack->current());
        self::assertSame($span, $stack->pop());
        self::assertTrue($stack->isEmpty());
    }

    public function test_remove_suite_span() : void
    {
        $stack = new SpanStack();
        $span = $this->createSpan('suite-span');

        $stack->setSuiteSpan('TestSuite', $span);
        $stack->removeSuiteSpan('TestSuite');

        self::assertNull($stack->getSuiteSpan('TestSuite'));
    }

    public function test_set_and_get_suite_span() : void
    {
        $stack = new SpanStack();
        $span = $this->createSpan('suite-span');

        $stack->setSuiteSpan('TestSuite', $span);

        self::assertSame($span, $stack->getSuiteSpan('TestSuite'));
        self::assertNull($stack->getSuiteSpan('OtherSuite'));
    }

    public function test_starts_empty() : void
    {
        $stack = new SpanStack();

        self::assertTrue($stack->isEmpty());
        self::assertNull($stack->current());
        self::assertNull($stack->pop());
    }

    private function createSpan(string $name) : Span
    {
        return new Span(
            $name,
            SpanContext::create(trace_id(), span_id()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            Resource::create([]),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
