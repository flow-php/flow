<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use function Flow\Telemetry\DSL\{memory_span_processor, void_span_exporter};

use Flow\Bridge\PHPUnit\Telemetry\{SpanStack, TestStatusRegistry};
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\{ConfigurationMother, TelemetryMother, TestEventMother};
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

final class TestFinishedSubscriberTest extends TestCase
{
    public function test_clears_status_registry_when_emit_test_spans_is_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $statusRegistry = new TestStatusRegistry();

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'failed', 'some error');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry);
        $subscriber->notify($event);

        self::assertNull($statusRegistry->getMessage($event->test()->id()));
    }

    public function test_does_not_pop_span_when_emit_test_spans_is_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
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

        self::assertFalse($spanStack->isEmpty());
        self::assertSame($suiteSpan, $spanStack->current());
    }

    public function test_pops_span_when_emit_test_spans_is_enabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
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

        self::assertTrue($spanStack->isEmpty());
        self::assertCount(1, $spanProcessor->endedSpans());
    }
}
