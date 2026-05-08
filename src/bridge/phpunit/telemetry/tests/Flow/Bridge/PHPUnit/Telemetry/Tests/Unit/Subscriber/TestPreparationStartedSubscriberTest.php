<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use function Flow\Telemetry\DSL\{memory_span_processor, void_exporter};

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestPreparationStartedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\{ConfigurationMother, TelemetryMother, TestEventMother};
use PHPUnit\Framework\TestCase;

final class TestPreparationStartedSubscriberTest extends TestCase
{
    public function test_creates_span_when_emit_test_spans_is_enabled() : void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config);
        $subscriber->notify(TestEventMother::preparationStarted());

        self::assertFalse($spanStack->isEmpty());
        self::assertCount(1, $spanProcessor->startedSpans());
    }

    public function test_does_not_create_span_when_emit_test_spans_is_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config);
        $subscriber->notify(TestEventMother::preparationStarted());

        self::assertTrue($spanStack->isEmpty());
        self::assertCount(0, $spanProcessor->startedSpans());
    }
}
