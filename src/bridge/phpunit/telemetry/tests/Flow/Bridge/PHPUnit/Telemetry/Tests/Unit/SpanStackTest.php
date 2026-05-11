<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

final class SpanStackTest extends TestCase
{
    public function test_clear_removes_all_spans(): void
    {
        $stack = new SpanStack();
        $span1 = SpanMother::create('span-1');
        $span2 = SpanMother::create('span-2');
        $suiteSpan = SpanMother::create('suite-span');

        $stack->push($span1);
        $stack->push($span2);
        $stack->setSuiteSpan('TestSuite', $suiteSpan);

        $stack->clear();

        static::assertTrue($stack->isEmpty());
        static::assertNull($stack->getSuiteSpan('TestSuite'));
    }

    public function test_current_returns_top_without_removing(): void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('test-span');

        $stack->push($span);

        static::assertSame($span, $stack->current());
        static::assertSame($span, $stack->current());
        static::assertFalse($stack->isEmpty());
    }

    public function test_lifo_order(): void
    {
        $stack = new SpanStack();
        $span1 = SpanMother::create('span-1');
        $span2 = SpanMother::create('span-2');
        $span3 = SpanMother::create('span-3');

        $stack->push($span1);
        $stack->push($span2);
        $stack->push($span3);

        static::assertSame($span3, $stack->pop());
        static::assertSame($span2, $stack->pop());
        static::assertSame($span1, $stack->pop());
        static::assertTrue($stack->isEmpty());
    }

    public function test_push_and_pop(): void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('test-span');

        $stack->push($span);

        static::assertFalse($stack->isEmpty());
        static::assertSame($span, $stack->current());
        static::assertSame($span, $stack->pop());
        static::assertTrue($stack->isEmpty());
    }

    public function test_remove_suite_span(): void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('suite-span');

        $stack->setSuiteSpan('TestSuite', $span);
        $stack->removeSuiteSpan('TestSuite');

        static::assertNull($stack->getSuiteSpan('TestSuite'));
    }

    public function test_set_and_get_suite_span(): void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('suite-span');

        $stack->setSuiteSpan('TestSuite', $span);

        static::assertSame($span, $stack->getSuiteSpan('TestSuite'));
        static::assertNull($stack->getSuiteSpan('OtherSuite'));
    }

    public function test_starts_empty(): void
    {
        $stack = new SpanStack();

        static::assertTrue($stack->isEmpty());
        static::assertNull($stack->current());
        static::assertNull($stack->pop());
    }
}
