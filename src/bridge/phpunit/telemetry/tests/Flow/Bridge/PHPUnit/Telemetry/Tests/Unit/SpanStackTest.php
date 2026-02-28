<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

final class SpanStackTest extends TestCase
{
    public function test_clear_removes_all_spans() : void
    {
        $stack = new SpanStack();
        $span1 = SpanMother::create('span-1');
        $span2 = SpanMother::create('span-2');
        $suiteSpan = SpanMother::create('suite-span');

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
        $span = SpanMother::create('test-span');

        $stack->push($span);

        self::assertSame($span, $stack->current());
        self::assertSame($span, $stack->current());
        self::assertFalse($stack->isEmpty());
    }

    public function test_lifo_order() : void
    {
        $stack = new SpanStack();
        $span1 = SpanMother::create('span-1');
        $span2 = SpanMother::create('span-2');
        $span3 = SpanMother::create('span-3');

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
        $span = SpanMother::create('test-span');

        $stack->push($span);

        self::assertFalse($stack->isEmpty());
        self::assertSame($span, $stack->current());
        self::assertSame($span, $stack->pop());
        self::assertTrue($stack->isEmpty());
    }

    public function test_remove_suite_span() : void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('suite-span');

        $stack->setSuiteSpan('TestSuite', $span);
        $stack->removeSuiteSpan('TestSuite');

        self::assertNull($stack->getSuiteSpan('TestSuite'));
    }

    public function test_set_and_get_suite_span() : void
    {
        $stack = new SpanStack();
        $span = SpanMother::create('suite-span');

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
}
