<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\DSL;

use function Flow\Telemetry\DSL\{baggage, context, instrumentation_scope, log_record_limits, logger_provider, memory_log_exporter, memory_metric_exporter, memory_span_exporter, meter_provider, metric_limits, resource, span_context, span_event, span_id, span_limits, span_link, trace_id, tracer_provider, void_log_processor, void_metric_processor, void_span_processor};
use Flow\Telemetry\Context\{Baggage, Context, MemoryContextStorage, SpanId, TraceId};
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Logger\{LogRecordLimits, LoggerProvider, Severity};
use Flow\Telemetry\Meter\{MeterProvider, MetricLimits};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{GenericEvent, SpanContext, SpanLimits, SpanLink, TracerProvider};
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class FunctionsTest extends TestCase
{
    private Resource $testResource;

    protected function setUp() : void
    {
        $this->testResource = ResourceMother::default();
    }

    public function test_baggage_creates_empty_baggage() : void
    {
        $bag = baggage();

        self::assertInstanceOf(Baggage::class, $bag);
        self::assertTrue($bag->isEmpty());
    }

    public function test_baggage_creates_with_entries() : void
    {
        $entries = ['user.id' => '12345', 'request.id' => 'abc'];
        $bag = baggage($entries);

        self::assertInstanceOf(Baggage::class, $bag);
        self::assertSame('12345', $bag->get('user.id'));
        self::assertSame('abc', $bag->get('request.id'));
    }

    public function test_context_creates_new_context() : void
    {
        $ctx = context();

        self::assertInstanceOf(Context::class, $ctx);
        self::assertSame(32, \strlen($ctx->traceId->toHex()));
        self::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_context_with_baggage() : void
    {
        $bag = baggage(['key' => 'value']);
        $ctx = context(null, $bag);

        self::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_context_with_trace_id() : void
    {
        $traceId = trace_id();
        $ctx = context($traceId);

        self::assertTrue($ctx->traceId->equals($traceId));
    }

    public function test_context_with_trace_id_and_baggage() : void
    {
        $traceId = trace_id();
        $bag = baggage(['key' => 'value']);
        $ctx = context($traceId, $bag);

        self::assertTrue($ctx->traceId->equals($traceId));
        self::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_instrumentation_scope_creates() : void
    {
        $scope = instrumentation_scope('my-lib');

        self::assertInstanceOf(InstrumentationScope::class, $scope);
        self::assertSame('my-lib', $scope->name);
        self::assertSame('unknown', $scope->version);
    }

    public function test_instrumentation_scope_with_version_and_schema() : void
    {
        $scope = instrumentation_scope('my-lib', '1.0.0', 'https://schema.url');

        self::assertSame('1.0.0', $scope->version);
        self::assertSame('https://schema.url', $scope->schemaUrl);
    }

    public function test_log_record_limits_creates_custom_limits() : void
    {
        $limits = log_record_limits(attributeCountLimit: 64, attributeValueLengthLimit: 100);

        self::assertSame(64, $limits->attributeCountLimit);
        self::assertSame(100, $limits->attributeValueLengthLimit);
    }

    public function test_log_record_limits_creates_default_limits() : void
    {
        $limits = log_record_limits();

        self::assertInstanceOf(LogRecordLimits::class, $limits);
        self::assertSame(128, $limits->attributeCountLimit);
    }

    public function test_logger_provider_creates_provider() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $processor = $this->createLogProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = logger_provider($processor, $clock, $contextStorage);

        self::assertInstanceOf(LoggerProvider::class, $provider);
    }

    public function test_logger_provider_works_correctly() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $clock->method('now')->willReturn(new \DateTimeImmutable());
        $processor = $this->createLogProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = logger_provider($processor, $clock, $contextStorage);
        $logger = $provider->logger($this->testResource, 'test');
        $logger->info('test message');

        self::assertCount(1, $processor->entries());
        self::assertSame(Severity::INFO, $processor->entries()[0]->record->severity);
    }

    public function test_memory_log_exporter_creates_instance() : void
    {
        self::assertInstanceOf(MemoryLogExporter::class, memory_log_exporter());
    }

    public function test_memory_metric_exporter_creates_instance() : void
    {
        self::assertInstanceOf(MemoryMetricExporter::class, memory_metric_exporter());
    }

    public function test_memory_span_exporter_creates_instance() : void
    {
        self::assertInstanceOf(MemorySpanExporter::class, memory_span_exporter());
    }

    public function test_meter_provider_creates_provider() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $processor = $this->createMetricProcessor();

        $provider = meter_provider($processor, $clock);

        self::assertInstanceOf(MeterProvider::class, $provider);
    }

    public function test_meter_provider_with_limits() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $clock->method('now')->willReturn(new \DateTimeImmutable());
        $processor = $this->createMetricProcessor();
        $limits = metric_limits(cardinalityLimit: 5);

        $provider = meter_provider($processor, $clock, limits: $limits);
        $meter = $provider->meter($this->testResource, 'test');
        $counter = $meter->createCounter('test.counter');

        for ($i = 1; $i <= 6; $i++) {
            $counter->add(1, ['id' => (string) $i]);
        }

        $metrics = $counter->collect();

        self::assertCount(6, $metrics);
        $overflowMetrics = \array_filter($metrics, static fn ($m) => $m->attributes->has(MetricLimits::OVERFLOW_ATTRIBUTE));
        self::assertCount(1, $overflowMetrics);
    }

    public function test_metric_limits_creates_custom_limits() : void
    {
        $limits = metric_limits(cardinalityLimit: 500);

        self::assertSame(500, $limits->cardinalityLimit);
    }

    public function test_metric_limits_creates_default_limits() : void
    {
        $limits = metric_limits();

        self::assertInstanceOf(MetricLimits::class, $limits);
        self::assertSame(2000, $limits->cardinalityLimit);
    }

    public function test_resource_creates_empty_resource() : void
    {
        $res = resource();

        self::assertInstanceOf(Resource::class, $res);
        self::assertTrue($res->isEmpty());
    }

    public function test_resource_creates_with_attributes() : void
    {
        $attributes = [
            'service.name' => 'my-service',
            'service.version' => '1.0.0',
        ];
        $res = resource($attributes);

        self::assertInstanceOf(Resource::class, $res);
        self::assertSame('my-service', $res->get('service.name'));
        self::assertSame('1.0.0', $res->get('service.version'));
    }

    public function test_span_context_creates_context() : void
    {
        $traceId = trace_id();
        $spanId = span_id();

        $context = span_context($traceId, $spanId);

        self::assertInstanceOf(SpanContext::class, $context);
        self::assertTrue($context->traceId->equals($traceId));
        self::assertTrue($context->spanId->equals($spanId));
        self::assertNull($context->parentSpanId);
    }

    public function test_span_context_with_parent() : void
    {
        $traceId = trace_id();
        $spanId = span_id();
        $parentSpanId = span_id();

        $context = span_context($traceId, $spanId, $parentSpanId);

        self::assertNotNull($context->parentSpanId);
        self::assertTrue($context->parentSpanId->equals($parentSpanId));
    }

    public function test_span_event_creates_event() : void
    {
        $timestamp = new \DateTimeImmutable();
        $event = span_event('test.event', $timestamp);

        self::assertInstanceOf(GenericEvent::class, $event);
        self::assertSame('test.event', $event->name());
        self::assertSame($timestamp, $event->timestamp());
        self::assertSame([], $event->attributes());
    }

    public function test_span_event_with_attributes() : void
    {
        $timestamp = new \DateTimeImmutable();
        $event = span_event('test.event', $timestamp, ['key' => 'value']);

        self::assertSame(['key' => 'value'], $event->attributes());
    }

    public function test_span_id_from_hex() : void
    {
        $hex = '00f067aa0ba902b7';
        $spanId = span_id($hex);

        self::assertInstanceOf(SpanId::class, $spanId);
        self::assertSame($hex, $spanId->toHex());
    }

    public function test_span_id_generates_new_span_id() : void
    {
        $spanId = span_id();

        self::assertInstanceOf(SpanId::class, $spanId);
        self::assertSame(16, \strlen($spanId->toHex()));
    }

    public function test_span_limits_creates_custom_limits() : void
    {
        $limits = span_limits(
            attributeCountLimit: 64,
            eventCountLimit: 32,
            linkCountLimit: 16,
        );

        self::assertSame(64, $limits->attributeCountLimit);
        self::assertSame(32, $limits->eventCountLimit);
        self::assertSame(16, $limits->linkCountLimit);
    }

    public function test_span_limits_creates_default_limits() : void
    {
        $limits = span_limits();

        self::assertInstanceOf(SpanLimits::class, $limits);
        self::assertSame(128, $limits->attributeCountLimit);
        self::assertSame(128, $limits->eventCountLimit);
        self::assertSame(128, $limits->linkCountLimit);
    }

    public function test_span_link_creates_link() : void
    {
        $context = span_context(trace_id(), span_id());

        $link = span_link($context);

        self::assertInstanceOf(SpanLink::class, $link);
        self::assertSame($context, $link->context);
        self::assertSame([], $link->attributes->normalize());
    }

    public function test_span_link_with_attributes() : void
    {
        $context = span_context(trace_id(), span_id());

        $link = span_link($context, ['reason' => 'batch']);

        self::assertSame(['reason' => 'batch'], $link->attributes->normalize());
    }

    public function test_trace_id_from_hex() : void
    {
        $hex = '0af7651916cd43dd8448eb211c80319c';
        $traceId = trace_id($hex);

        self::assertInstanceOf(TraceId::class, $traceId);
        self::assertSame($hex, $traceId->toHex());
    }

    public function test_trace_id_generates_new_trace_id() : void
    {
        $traceId = trace_id();

        self::assertInstanceOf(TraceId::class, $traceId);
        self::assertSame(32, \strlen($traceId->toHex()));
    }

    public function test_tracer_provider_creates_provider() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $processor = $this->createSpanProcessor();
        $contextStorage = new MemoryContextStorage();

        $provider = tracer_provider($processor, $clock, $contextStorage);

        self::assertInstanceOf(TracerProvider::class, $provider);
    }

    public function test_tracer_provider_with_context_storage() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $clock->method('now')->willReturn(new \DateTimeImmutable());
        $ctx = context();
        $storage = new MemoryContextStorage($ctx);
        $processor = $this->createSpanProcessor();

        $provider = tracer_provider($processor, $clock, $storage);
        $tracer = $provider->tracer($this->testResource, 'test');

        self::assertSame($ctx->traceId->toHex(), $tracer->context()->traceId->toHex());
    }

    public function test_tracer_provider_with_void_processor() : void
    {
        $clock = $this->createMock(ClockInterface::class);
        $contextStorage = new MemoryContextStorage();

        $provider = tracer_provider(new VoidSpanProcessor(), $clock, $contextStorage);

        self::assertInstanceOf(TracerProvider::class, $provider);
    }

    public function test_void_log_processor_creates_instance() : void
    {
        self::assertInstanceOf(VoidLogProcessor::class, void_log_processor());
    }

    public function test_void_metric_processor_creates_instance() : void
    {
        self::assertInstanceOf(VoidMetricProcessor::class, void_metric_processor());
    }

    public function test_void_span_processor_creates_instance() : void
    {
        self::assertInstanceOf(VoidSpanProcessor::class, void_span_processor());
    }

    private function createLogProcessor() : MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidLogExporter());
    }

    private function createMetricProcessor() : MemoryMetricProcessor
    {
        return new MemoryMetricProcessor(new VoidMetricExporter());
    }

    private function createSpanProcessor() : MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidSpanExporter());
    }
}
