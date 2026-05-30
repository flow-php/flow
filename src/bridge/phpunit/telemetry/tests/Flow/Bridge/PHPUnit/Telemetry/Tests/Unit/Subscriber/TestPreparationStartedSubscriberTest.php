<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestPreparationStartedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\TestMemoryRegistry;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TelemetryMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TestEventMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

final class TestPreparationStartedSubscriberTest extends TestCase
{
    public function test_creates_span_when_emit_test_spans_is_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $memoryRegistry = new TestMemoryRegistry();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config, $memoryRegistry);
        $subscriber->notify(TestEventMother::preparationStarted());

        static::assertFalse($spanStack->isEmpty());
        static::assertCount(1, $spanProcessor->startedSpans());
    }

    public function test_does_not_create_span_when_emit_test_spans_is_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $memoryRegistry = new TestMemoryRegistry();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config, $memoryRegistry);
        $subscriber->notify(TestEventMother::preparationStarted());

        static::assertTrue($spanStack->isEmpty());
        static::assertCount(0, $spanProcessor->startedSpans());
    }

    public function test_records_memory_start_when_spans_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $memoryRegistry = new TestMemoryRegistry();

        $event = TestEventMother::preparationStarted();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config, $memoryRegistry);
        $subscriber->notify($event);

        static::assertNotNull($memoryRegistry->getStart($event->test()->id()));
    }

    public function test_records_memory_start_when_only_metrics_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $memoryRegistry = new TestMemoryRegistry();

        $event = TestEventMother::preparationStarted();

        $subscriber = new TestPreparationStartedSubscriber($telemetry, $spanStack, $config, $memoryRegistry);
        $subscriber->notify($event);

        static::assertTrue($spanStack->isEmpty());
        static::assertNotNull($memoryRegistry->getStart($event->test()->id()));
    }
}
