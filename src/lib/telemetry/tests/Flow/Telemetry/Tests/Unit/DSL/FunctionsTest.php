<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\DSL;

use DateTimeImmutable;
use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\LogRecordLimits;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricLimits;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanLimits;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

use function array_filter;
use function Flow\Telemetry\DSL\baggage;
use function Flow\Telemetry\DSL\context;
use function Flow\Telemetry\DSL\instrumentation_scope;
use function Flow\Telemetry\DSL\log_record_limits;
use function Flow\Telemetry\DSL\logger_provider;
use function Flow\Telemetry\DSL\memory_exporter;
use function Flow\Telemetry\DSL\meter_provider;
use function Flow\Telemetry\DSL\metric_limits;
use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\span_context;
use function Flow\Telemetry\DSL\span_event;
use function Flow\Telemetry\DSL\span_id;
use function Flow\Telemetry\DSL\span_limits;
use function Flow\Telemetry\DSL\span_link;
use function Flow\Telemetry\DSL\trace_id;
use function Flow\Telemetry\DSL\tracer_provider;
use function Flow\Telemetry\DSL\void_log_processor;
use function Flow\Telemetry\DSL\void_metric_processor;
use function Flow\Telemetry\DSL\void_span_processor;
use function strlen;

final class FunctionsTest extends TestCase
{
    private Resource $testResource;

    protected function setUp(): void
    {
        $this->testResource = ResourceMother::default();
    }

    public function test_baggage_creates_empty_baggage(): void
    {
        $bag = baggage();

        static::assertInstanceOf(Baggage::class, $bag);
        static::assertTrue($bag->isEmpty());
    }

    public function test_baggage_creates_with_entries(): void
    {
        $entries = ['user.id' => '12345', 'request.id' => 'abc'];
        $bag = baggage($entries);

        static::assertInstanceOf(Baggage::class, $bag);
        static::assertSame('12345', $bag->get('user.id'));
        static::assertSame('abc', $bag->get('request.id'));
    }

    public function test_context_creates_root_context(): void
    {
        $ctx = context();

        static::assertInstanceOf(Context::class, $ctx);
        static::assertTrue($ctx->isRootContext());
        static::assertNull($ctx->traceId());
        static::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_context_with_baggage(): void
    {
        $ctx = context(baggage(['key' => 'value']));

        static::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_instrumentation_scope_creates(): void
    {
        $scope = instrumentation_scope('my-lib');

        static::assertInstanceOf(InstrumentationScope::class, $scope);
        static::assertSame('my-lib', $scope->name);
        static::assertSame('unknown', $scope->version);
    }

    public function test_instrumentation_scope_with_version_and_schema(): void
    {
        $scope = instrumentation_scope('my-lib', '1.0.0', 'https://schema.url');

        static::assertSame('1.0.0', $scope->version);
        static::assertSame('https://schema.url', $scope->schemaUrl);
    }

    public function test_log_record_limits_creates_custom_limits(): void
    {
        $limits = log_record_limits(attributeCountLimit: 64, attributeValueLengthLimit: 100);

        static::assertSame(64, $limits->attributeCountLimit);
        static::assertSame(100, $limits->attributeValueLengthLimit);
    }

    public function test_log_record_limits_creates_default_limits(): void
    {
        $limits = log_record_limits();

        static::assertInstanceOf(LogRecordLimits::class, $limits);
        static::assertSame(128, $limits->attributeCountLimit);
    }

    public function test_logger_provider_creates_provider(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $processor = $this->createLogProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = logger_provider($processor, $clock, $contextStorage);

        static::assertInstanceOf(LoggerProvider::class, $provider);
    }

    public function test_logger_provider_works_correctly(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $clock->method('now')->willReturn(new DateTimeImmutable());
        $processor = $this->createLogProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = logger_provider($processor, $clock, $contextStorage);
        $logger = $provider->logger($this->testResource, 'test');
        $logger->info('test message');

        static::assertCount(1, $processor->entries());
        static::assertSame(Severity::INFO, $processor->entries()[0]->record->severity);
    }

    public function test_memory_exporter_creates_instance(): void
    {
        static::assertInstanceOf(MemoryExporter::class, memory_exporter());
    }

    public function test_meter_provider_creates_provider(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $processor = $this->createMetricProcessor();

        $provider = meter_provider($processor, $clock);

        static::assertInstanceOf(MeterProvider::class, $provider);
    }

    public function test_meter_provider_with_limits(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $clock->method('now')->willReturn(new DateTimeImmutable());
        $processor = $this->createMetricProcessor();
        $limits = metric_limits(cardinalityLimit: 5);

        $provider = meter_provider($processor, $clock, limits: $limits);
        $meter = $provider->meter($this->testResource, 'test');
        $counter = $meter->createCounter('test.counter');

        for ($i = 1; $i <= 6; $i++) {
            $counter->add(1, ['id' => (string) $i]);
        }

        $metrics = $counter->collect();

        static::assertCount(6, $metrics);
        $overflowMetrics = array_filter($metrics, static fn($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE));
        static::assertCount(1, $overflowMetrics);
    }

    public function test_metric_limits_creates_custom_limits(): void
    {
        $limits = metric_limits(cardinalityLimit: 500);

        static::assertSame(500, $limits->cardinalityLimit);
    }

    public function test_metric_limits_creates_default_limits(): void
    {
        $limits = metric_limits();

        static::assertInstanceOf(MetricLimits::class, $limits);
        static::assertSame(2000, $limits->cardinalityLimit);
    }

    public function test_resource_creates_empty_resource(): void
    {
        $res = resource();

        static::assertInstanceOf(Resource::class, $res);
        static::assertTrue($res->isEmpty());
    }

    public function test_resource_creates_with_attributes(): void
    {
        $attributes = [
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ];
        $res = resource($attributes);

        static::assertInstanceOf(Resource::class, $res);
        static::assertSame('my-service', $res->get('service.name'));
        static::assertSame('1.0.0', $res->get('service.version'));
    }

    public function test_span_context_creates_context(): void
    {
        $traceId = trace_id();
        $spanId = span_id();

        $context = span_context($traceId, $spanId);

        static::assertInstanceOf(SpanContext::class, $context);
        static::assertTrue($context->traceId->equals($traceId));
        static::assertTrue($context->spanId->equals($spanId));
        static::assertNull($context->parentSpanId);
    }

    public function test_span_context_with_parent(): void
    {
        $traceId = trace_id();
        $spanId = span_id();
        $parentSpanId = span_id();

        $context = span_context($traceId, $spanId, $parentSpanId);

        static::assertNotNull($context->parentSpanId);
        static::assertTrue($context->parentSpanId->equals($parentSpanId));
    }

    public function test_span_event_creates_event(): void
    {
        $timestamp = new DateTimeImmutable();
        $event = span_event('test.event', $timestamp);

        static::assertInstanceOf(GenericEvent::class, $event);
        static::assertSame('test.event', $event->name());
        static::assertSame($timestamp, $event->timestamp());
        static::assertSame([], $event->attributes());
    }

    public function test_span_event_with_attributes(): void
    {
        $timestamp = new DateTimeImmutable();
        $event = span_event('test.event', $timestamp, ['key' => 'value']);

        static::assertSame(['key' => 'value'], $event->attributes());
    }

    public function test_span_id_from_hex(): void
    {
        $hex = '00f067aa0ba902b7';
        $spanId = span_id($hex);

        static::assertInstanceOf(SpanId::class, $spanId);
        static::assertSame($hex, $spanId->toHex());
    }

    public function test_span_id_generates_new_span_id(): void
    {
        $spanId = span_id();

        static::assertInstanceOf(SpanId::class, $spanId);
        static::assertSame(16, strlen($spanId->toHex()));
    }

    public function test_span_limits_creates_custom_limits(): void
    {
        $limits = span_limits(attributeCountLimit: 64, eventCountLimit: 32, linkCountLimit: 16);

        static::assertSame(64, $limits->attributeCountLimit);
        static::assertSame(32, $limits->eventCountLimit);
        static::assertSame(16, $limits->linkCountLimit);
    }

    public function test_span_limits_creates_default_limits(): void
    {
        $limits = span_limits();

        static::assertInstanceOf(SpanLimits::class, $limits);
        static::assertSame(128, $limits->attributeCountLimit);
        static::assertSame(128, $limits->eventCountLimit);
        static::assertSame(128, $limits->linkCountLimit);
    }

    public function test_span_link_creates_link(): void
    {
        $context = span_context(trace_id(), span_id());

        $link = span_link($context);

        static::assertInstanceOf(SpanLink::class, $link);
        static::assertSame($context, $link->context);
        static::assertSame([], $link->attributes->normalize());
    }

    public function test_span_link_with_attributes(): void
    {
        $context = span_context(trace_id(), span_id());

        $link = span_link($context, ['reason' => 'batch']);

        static::assertSame(['reason' => 'batch'], $link->attributes->normalize());
    }

    public function test_trace_id_from_hex(): void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = trace_id($hex);

        static::assertInstanceOf(TraceId::class, $traceId);
        static::assertSame($hex, $traceId->toHex());
    }

    public function test_trace_id_generates_new_trace_id(): void
    {
        $traceId = trace_id();

        static::assertInstanceOf(TraceId::class, $traceId);
        static::assertSame(32, strlen($traceId->toHex()));
    }

    public function test_tracer_provider_creates_provider(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $processor = $this->createSpanProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = tracer_provider($processor, $clock, $contextStorage);

        static::assertInstanceOf(TracerProvider::class, $provider);
    }

    public function test_tracer_provider_with_context_storage(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $clock->method('now')->willReturn(new DateTimeImmutable());
        $span = span_context(trace_id(), span_id());
        $storage = new MemoryContextStorage(context()->withActiveSpan($span));
        $processor = $this->createSpanProcessor();

        $provider = tracer_provider($processor, $clock, $storage);
        $tracer = $provider->tracer($this->testResource, 'test');

        static::assertSame($span->spanId->toHex(), $tracer->context()->activeSpanId()?->toHex());
    }

    public function test_tracer_provider_with_void_processor(): void
    {
        $clock = $this->createStub(ClockInterface::class);
        $contextStorage = new MemoryContextStorage();

        $provider = tracer_provider(new VoidSpanProcessor(), $clock, $contextStorage);

        static::assertInstanceOf(TracerProvider::class, $provider);
    }

    public function test_void_log_processor_creates_instance(): void
    {
        static::assertInstanceOf(VoidLogProcessor::class, void_log_processor());
    }

    public function test_void_metric_processor_creates_instance(): void
    {
        static::assertInstanceOf(VoidMetricProcessor::class, void_metric_processor());
    }

    public function test_void_span_processor_creates_instance(): void
    {
        static::assertInstanceOf(VoidSpanProcessor::class, void_span_processor());
    }

    private function createLogProcessor(): MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidExporter());
    }

    private function createMetricProcessor(): MemoryMetricProcessor
    {
        return new MemoryMetricProcessor(new VoidExporter());
    }

    private function createSpanProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }
}
