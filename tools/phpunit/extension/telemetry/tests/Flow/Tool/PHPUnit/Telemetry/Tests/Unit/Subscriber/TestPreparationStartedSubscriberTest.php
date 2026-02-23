<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanExporter;
use Flow\Tool\PHPUnit\Telemetry\SpanStack;
use Flow\Tool\PHPUnit\Telemetry\Subscriber\TestPreparationStartedSubscriber;
use Flow\Tool\PHPUnit\Telemetry\Tests\Mother\{ConfigurationMother, TelemetryMother, TestEventMother};
use PHPUnit\Framework\TestCase;

final class TestPreparationStartedSubscriberTest extends TestCase
{
    public function test_creates_span_when_emit_test_spans_is_enabled() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidSpanExporter());
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
        $spanProcessor = new MemorySpanProcessor(new VoidSpanExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config);
        $subscriber->notify(TestEventMother::preparationStarted());

        self::assertTrue($spanStack->isEmpty());
        self::assertCount(0, $spanProcessor->startedSpans());
    }
}
