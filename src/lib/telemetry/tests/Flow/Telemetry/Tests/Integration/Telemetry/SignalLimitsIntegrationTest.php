<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Telemetry;

use DateTimeImmutable;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\LogRecordLimits;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricLimits;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanLimits;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

use function array_filter;
use function array_values;

/**
 * Integration tests for signal attribute limits.
 *
 * These tests verify the complete path from Provider → Signal → Processor
 * with custom limits configured.
 */
final class SignalLimitsIntegrationTest extends TestCase
{
    private ClockInterface $clock;

    protected function setUp(): void
    {
        $this->clock = $this->createStub(ClockInterface::class);
        $this->clock->method('now')->willReturn(new DateTimeImmutable('2024-01-01 12:00:00.123456'));
    }

    public function test_logger_dropped_count_included_in_normalized_output(): void
    {
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new LogRecordLimits(attributeCountLimit: 1);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage, $limits);

        $logger = $loggerProvider->logger(ResourceMother::default(), 'test-service');

        $logger->info('Test message', ['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);

        $normalized = $logProcessor->entries()[0]->normalize();

        static::assertSame(2, $normalized['droppedAttributeCount']);
    }

    public function test_logger_enforces_attribute_count_limit(): void
    {
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new LogRecordLimits(attributeCountLimit: 3);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage, $limits);

        $logger = $loggerProvider->logger(ResourceMother::default(), 'test-service');

        $logger->info('Test message', [
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
            'key4' => 'value4',
            'key5' => 'value5',
        ]);

        $entries = $logProcessor->entries();
        static::assertCount(1, $entries);

        $entry = $entries[0];
        static::assertCount(3, $entry->record->attributes->normalize());
        static::assertSame(2, $entry->droppedAttributeCount);
        static::assertArrayHasKey('key1', $entry->record->attributes->normalize());
        static::assertArrayHasKey('key2', $entry->record->attributes->normalize());
        static::assertArrayHasKey('key3', $entry->record->attributes->normalize());
    }

    public function test_logger_enforces_attribute_value_length_limit(): void
    {
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new LogRecordLimits(attributeValueLengthLimit: 10);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage, $limits);

        $logger = $loggerProvider->logger(ResourceMother::default(), 'test-service');

        $logger->info('Test message', [
            'short' => 'abc',
            'long' => 'this-is-a-very-long-string-value',
        ]);

        $entries = $logProcessor->entries();
        $attrs = $entries[0]->record->attributes->normalize();

        static::assertSame('abc', $attrs['short']);
        static::assertSame('this-is-a-', $attrs['long']);
    }

    public function test_meter_counter_enforces_cardinality_limit(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $limits = new MetricLimits(cardinalityLimit: 3);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock, limits: $limits);

        $meter = $meterProvider->meter(ResourceMother::default(), 'test-service');
        $counter = $meter->createCounter('test.counter');

        $counter->add(1, ['key' => 'value1']);
        $counter->add(2, ['key' => 'value2']);
        $counter->add(3, ['key' => 'value3']);
        $counter->add(4, ['key' => 'value4']);
        $counter->add(5, ['key' => 'value5']);

        $metrics = $counter->collect();

        static::assertCount(4, $metrics);

        $overflowMetrics = array_values(array_filter($metrics, static fn($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE)));
        static::assertCount(1, $overflowMetrics);

        static::assertTrue($overflowMetrics[0]->attributes->get(MetricLimits::OVERFLOW_ATTRIBUTE));
        static::assertSame(9, $overflowMetrics[0]->value);
    }

    public function test_meter_histogram_enforces_cardinality_limit(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $limits = new MetricLimits(cardinalityLimit: 2);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock, limits: $limits);

        $meter = $meterProvider->meter(ResourceMother::default(), 'test-service');
        $histogram = $meter->createHistogram('test.histogram');

        $histogram->record(10.0, ['region' => 'us']);
        $histogram->record(20.0, ['region' => 'eu']);
        $histogram->record(30.0, ['region' => 'asia']);
        $histogram->record(40.0, ['region' => 'africa']);

        $metrics = $histogram->collect();

        static::assertCount(3, $metrics);

        $overflowMetrics = array_values(array_filter($metrics, static fn($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE)));
        static::assertCount(1, $overflowMetrics);
    }

    public function test_meter_overflow_aggregates_excess_measurements(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $limits = new MetricLimits(cardinalityLimit: 2);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock, limits: $limits);

        $meter = $meterProvider->meter(ResourceMother::default(), 'test-service');
        $counter = $meter->createCounter('test.counter');

        $counter->add(1, ['id' => '1']);
        $counter->add(2, ['id' => '2']);
        $counter->add(3, ['id' => '3']);
        $counter->add(4, ['id' => '4']);
        $counter->add(5, ['id' => '5']);

        $metrics = $counter->collect();

        $overflowMetrics = array_values(array_filter($metrics, static fn($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE)));

        static::assertSame(12, $overflowMetrics[0]->value);
    }

    public function test_meter_overflow_attribute_set_has_correct_attribute(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $limits = new MetricLimits(cardinalityLimit: 1);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock, limits: $limits);

        $meter = $meterProvider->meter(ResourceMother::default(), 'test-service');
        $counter = $meter->createCounter('test.counter');

        $counter->add(1, ['key' => 'first']);
        $counter->add(2, ['key' => 'overflow']);

        $metrics = $counter->collect();

        static::assertCount(2, $metrics);

        $overflowMetrics = array_values(array_filter($metrics, static fn($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE)));

        static::assertTrue($overflowMetrics[0]->attributes->get(MetricLimits::OVERFLOW_ATTRIBUTE));
        static::assertCount(1, $overflowMetrics[0]->attributes->normalize());
    }

    public function test_meter_reuses_existing_aggregations_within_limit(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $limits = new MetricLimits(cardinalityLimit: 2);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock, limits: $limits);

        $meter = $meterProvider->meter(ResourceMother::default(), 'test-service');
        $counter = $meter->createCounter('test.counter');

        $counter->add(1, ['key' => 'a']);
        $counter->add(2, ['key' => 'b']);
        $counter->add(3, ['key' => 'a']);
        $counter->add(4, ['key' => 'b']);

        $metrics = $counter->collect();

        static::assertCount(2, $metrics);

        $attrA = array_values(array_filter($metrics, static fn($m) => $m->attributes->get('key') === 'a'));
        $attrB = array_values(array_filter($metrics, static fn($m) => $m->attributes->get('key') === 'b'));

        static::assertSame(4, $attrA[0]->value);
        static::assertSame(6, $attrB[0]->value);
    }

    public function test_telemetry_facade_respects_limits(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();

        $spanLimits = new SpanLimits(attributeCountLimit: 2);
        $logLimits = new LogRecordLimits(attributeCountLimit: 2);

        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $spanLimits);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage, $logLimits);

        $telemetry = new Telemetry(ResourceMother::default(), $tracerProvider, $meterProvider, $loggerProvider);

        $tracer = $telemetry->tracer('test-service');
        $logger = $telemetry->logger('test-service');

        $span = $tracer->span('operation');
        $span->setAttributes(['a' => '1', 'b' => '2', 'c' => '3']);
        $tracer->complete($span);

        $logger->info('message', ['x' => '1', 'y' => '2', 'z' => '3']);

        static::assertSame(1, $spanProcessor->endedSpans()[0]->droppedAttributeCount());
        static::assertSame(1, $logProcessor->entries()[0]->droppedAttributeCount);
    }

    public function test_tracer_dropped_counts_included_in_normalized_output(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(attributeCountLimit: 2, eventCountLimit: 1, linkCountLimit: 1);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $span->setAttributes(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);
        $span->recordEvent(GenericEvent::create('e1', new DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('e2', new DateTimeImmutable()));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $tracer->complete($span);

        $normalized = $spanProcessor->endedSpans()[0]->normalize();

        static::assertSame(1, $normalized['droppedAttributeCount']);
        static::assertSame(1, $normalized['droppedEventsCount']);
        static::assertSame(1, $normalized['droppedLinksCount']);
    }

    public function test_tracer_enforces_event_attribute_limits(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(attributePerEventCountLimit: 2, attributeValueLengthLimit: 10);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $event = GenericEvent::create('test.event', new DateTimeImmutable(), [
            'key1' => 'short',
            'key2' => 'this-is-a-very-long-value',
            'key3' => 'dropped-attribute',
        ]);
        $span->recordEvent($event);
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        $recordedEvent = $endedSpans[0]->events()[0];

        static::assertCount(2, $recordedEvent->attributes());
        static::assertSame('short', $recordedEvent->attributes()['key1']);
        static::assertSame('this-is-a-', $recordedEvent->attributes()['key2']);
        static::assertSame(1, $recordedEvent->droppedAttributeCount());
    }

    public function test_tracer_enforces_event_count_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(eventCountLimit: 2);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $span->recordEvent(GenericEvent::create('event1', new DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('event2', new DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('event3', new DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('event4', new DateTimeImmutable()));
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        $recordedSpan = $endedSpans[0];

        static::assertCount(2, $recordedSpan->events());
        static::assertSame(2, $recordedSpan->droppedEventsCount());
        static::assertSame('event1', $recordedSpan->events()[0]->name());
        static::assertSame('event2', $recordedSpan->events()[1]->name());
    }

    public function test_tracer_enforces_link_attribute_limits(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(attributePerLinkCountLimit: 2, attributeValueLengthLimit: 10);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $link = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()), [
            'key1' => 'short',
            'key2' => 'this-is-a-very-long-value',
            'key3' => 'dropped-attribute',
        ]);
        $span->addLink($link);
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        $recordedLink = $endedSpans[0]->links()[0];

        static::assertCount(2, $recordedLink->attributes->normalize());
        static::assertSame('short', $recordedLink->attributes->normalize()['key1']);
        static::assertSame('this-is-a-', $recordedLink->attributes->normalize()['key2']);
        static::assertSame(1, $recordedLink->droppedAttributeCount);
    }

    public function test_tracer_enforces_link_count_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(linkCountLimit: 2);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        $recordedSpan = $endedSpans[0];

        static::assertCount(2, $recordedSpan->links());
        static::assertSame(1, $recordedSpan->droppedLinksCount());
    }

    public function test_tracer_enforces_span_attribute_count_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(attributeCountLimit: 3);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $span->setAttributes([
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
            'key4' => 'value4',
            'key5' => 'value5',
        ]);
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);

        $recordedSpan = $endedSpans[0];
        static::assertCount(3, $recordedSpan->attributes());
        static::assertSame(2, $recordedSpan->droppedAttributeCount());
        static::assertArrayHasKey('key1', $recordedSpan->attributes());
        static::assertArrayHasKey('key2', $recordedSpan->attributes());
        static::assertArrayHasKey('key3', $recordedSpan->attributes());
    }

    public function test_tracer_enforces_span_attribute_value_length_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $limits = new SpanLimits(attributeValueLengthLimit: 10);
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage, limits: $limits);

        $tracer = $tracerProvider->tracer(ResourceMother::default(), 'test-service');

        $span = $tracer->span('test-operation');
        $span->setAttribute('short', 'abc');
        $span->setAttribute('long', 'this-is-a-very-long-string-value');
        $tracer->complete($span);

        $endedSpans = $spanProcessor->endedSpans();
        $recordedSpan = $endedSpans[0];

        static::assertSame('abc', $recordedSpan->attributes()['short']);
        static::assertSame('this-is-a-', $recordedSpan->attributes()['long']);
    }
}
