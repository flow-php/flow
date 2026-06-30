<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\TestMemoryRegistry;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TelemetryMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TestEventMother;
use Flow\Bridge\PHPUnit\Telemetry\TestStatusRegistry;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\memory_metric_processor;
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
        $memoryRegistry = new TestMemoryRegistry();

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'failed', 'some error');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);

        static::assertNull($statusRegistry->getMessage($event->test()->id()));
    }

    public function test_clears_memory_registry_after_finish(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $testSpan = SpanMother::create('test-span');
        $spanStack->push($testSpan);

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');
        $memoryRegistry->setStart($event->test()->id(), 1024);

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);

        static::assertNull($memoryRegistry->getStart($event->test()->id()));
    }

    public function test_does_not_pop_span_when_emit_test_spans_is_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $suiteSpan = SpanMother::create('suite-span');
        $spanStack->push($suiteSpan);

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
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
        $memoryRegistry = new TestMemoryRegistry();

        $testSpan = SpanMother::create('test-span');
        $spanStack->push($testSpan);

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);

        static::assertTrue($spanStack->isEmpty());
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_records_memory_metrics_when_metrics_enabled(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $telemetry = TelemetryMother::withMetricProcessor($metricProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $spanStack->push(SpanMother::create('test-span'));

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');
        $memoryRegistry->setStart($event->test()->id(), 1024);

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);
        $telemetry->flush();

        static::assertNotEmpty($metricProcessor->metricsWithName('flow.phpunit.test.memory.peak'));
        static::assertNotEmpty($metricProcessor->metricsWithName('flow.phpunit.test.memory.delta'));
    }

    public function test_records_memory_delta_metric_when_only_metrics_enabled(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $telemetry = TelemetryMother::withMetricProcessor($metricProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::withDisabledTestSpans();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');
        $memoryRegistry->setStart($event->test()->id(), 1024);

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);
        $telemetry->flush();

        static::assertNotEmpty($metricProcessor->metricsWithName('flow.phpunit.test.memory.peak'));
        static::assertNotEmpty($metricProcessor->metricsWithName('flow.phpunit.test.memory.delta'));
    }

    public function test_sets_error_status_and_message_on_span_for_failed_test(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $spanStack->push(SpanMother::create('test-span'));

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'failed', 'some error');

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);

        $span = $spanProcessor->endedSpans()[0];
        static::assertTrue($span->status()?->isError());
        static::assertSame('failed', $span->attributes()['error.type']);
        static::assertArrayHasKey('exception.message', $span->attributes());
        static::assertSame('some error', $span->attributes()['exception.message']);
    }

    public function test_sets_memory_attributes_on_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $statusRegistry = new TestStatusRegistry();
        $memoryRegistry = new TestMemoryRegistry();

        $spanStack->push(SpanMother::create('test-span'));

        $event = TestEventMother::finished();
        $statusRegistry->setStatus($event->test()->id(), 'passed');
        $memoryRegistry->setStart($event->test()->id(), 1024);

        $subscriber = new TestFinishedSubscriber($telemetry, $spanStack, $config, $statusRegistry, $memoryRegistry);
        $subscriber->notify($event);

        $attributes = $spanProcessor->endedSpans()[0]->attributes();
        static::assertArrayHasKey('test.memory.peak_bytes', $attributes);
        static::assertArrayHasKey('test.memory.delta_bytes', $attributes);
    }
}
