<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\DSL;

use function Flow\Telemetry\DSL\{baggage, context, instrumentation_scope, logger_provider, resource, span_context, span_event, span_id, span_link, trace_id, tracer_provider};
use Flow\Telemetry\Context\{Baggage, Context, MemoryContextStorage, SpanId, TraceId};
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Logger\{LoggerProvider, Severity};
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{GenericEvent, SpanContext, SpanLink, TracerProvider};
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
        $event = span_event('test.event');

        self::assertInstanceOf(GenericEvent::class, $event);
        self::assertSame('test.event', $event->name());
        self::assertSame([], $event->attributes());
    }

    public function test_span_event_with_attributes() : void
    {
        $event = span_event('test.event', ['key' => 'value']);

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

    private function createLogProcessor() : MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidLogExporter());
    }

    private function createSpanProcessor() : MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidSpanExporter());
    }
}
