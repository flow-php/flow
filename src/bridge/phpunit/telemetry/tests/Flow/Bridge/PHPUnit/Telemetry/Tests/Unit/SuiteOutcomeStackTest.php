<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\SuiteOutcomeStack;
use PHPUnit\Framework\TestCase;

final class SuiteOutcomeStackTest extends TestCase
{
    public function test_empty_suite_is_success(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();

        static::assertSame('success', $stack->pop());
    }

    public function test_errored_test_makes_suite_a_failure(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('passed');
        $stack->recordStatus('errored');

        static::assertSame('failure', $stack->pop());
    }

    public function test_failed_test_makes_suite_a_failure(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('passed');
        $stack->recordStatus('failed');

        static::assertSame('failure', $stack->pop());
    }

    public function test_incomplete_tests_leave_suite_a_success(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('incomplete');
        $stack->recordStatus('passed');

        static::assertSame('success', $stack->pop());
    }

    public function test_nested_suites_aggregate_independently(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('failed');
        $stack->push();
        $stack->recordStatus('passed');

        static::assertSame('success', $stack->pop());
        static::assertSame('failure', $stack->pop());
    }

    public function test_only_skipped_tests_make_suite_skipped(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('skipped');
        $stack->recordStatus('skipped');

        static::assertSame('skipped', $stack->pop());
    }

    public function test_pop_without_open_frame_returns_null(): void
    {
        static::assertNull((new SuiteOutcomeStack())->pop());
    }

    public function test_size_reflects_open_frames(): void
    {
        $stack = new SuiteOutcomeStack();
        static::assertSame(0, $stack->size());

        $stack->push();
        $stack->push();
        static::assertSame(2, $stack->size());

        $stack->pop();
        static::assertSame(1, $stack->size());
    }

    public function test_statuses_propagate_to_all_open_frames(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->push();
        $stack->recordStatus('failed');

        static::assertSame('failure', $stack->pop());
        static::assertSame('failure', $stack->pop());
    }

    public function test_suite_with_skipped_and_passed_tests_is_success(): void
    {
        $stack = new SuiteOutcomeStack();
        $stack->push();
        $stack->recordStatus('skipped');
        $stack->recordStatus('passed');

        static::assertSame('success', $stack->pop());
    }
}
