<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TelemetryMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TestEventMother;
use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

final class TestFinishedSubscriberTest extends TestCase
{
    public function test_clears_status_registry_when_emit_test_spans_is_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $statusRegistry = new TestStatusRegistry();

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'failed', 'some error');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry);
        $subscriber->notify($event);

        static::assertNull($statusRegistry->getMessage($event->test()->id()));
    }

    public function test_does_not_pop_span_when_emit_test_spans_is_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $statusRegistry = new TestStatusRegistry();

        $suiteSpan = SpanMother::create('suite-span');
        $spanStack->push($suiteSpan);

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry);
        $subscriber->notify($event);

        static::assertFalse($spanStack->isEmpty());
        static::assertSame($suiteSpan, $spanStack->current());
    }

    public function test_pops_span_when_emit_test_spans_is_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $statusRegistry = new TestStatusRegistry();

        $testSpan = SpanMother::create('test-span');
        $spanStack->push($testSpan);

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry);
        $subscriber->notify($event);

        static::assertTrue($spanStack->isEmpty());
        static::assertCount(1, $spanProcessor->endedSpans());
    }
}
