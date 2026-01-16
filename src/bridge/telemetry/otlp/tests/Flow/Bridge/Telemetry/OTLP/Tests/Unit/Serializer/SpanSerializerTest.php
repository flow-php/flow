<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Serializer;

use Flow\Bridge\Telemetry\OTLP\Serializer\SpanSerializer;
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{GenericEvent, Span, SpanContext, SpanKind, SpanLink, SpanStatus};
use PHPUnit\Framework\TestCase;

final class SpanSerializerTest extends TestCase
{
    private SpanSerializer $serializer;

    protected function setUp() : void
    {
        $this->serializer = new SpanSerializer();
    }

    public function test_serialize_basic_span() : void
    {
        $traceId = TraceId::fromHex('0102030405060708090a0b0c0d0e0f10');
        $spanId = SpanId::fromHex('0102030405060708');
        $context = SpanContext::create($traceId, $spanId);
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00.000000');
        $scope = new InstrumentationScope('test', '1.0.0');

        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, ResourceMother::default(), $scope);

        $result = $this->serializer->serialize($span);

        self::assertSame('0102030405060708090a0b0c0d0e0f10', $result['traceId']);
        self::assertSame('0102030405060708', $result['spanId']);
        self::assertSame('test-span', $result['name']);
        self::assertSame(1, $result['kind']);
        self::assertSame([], $result['attributes']);
        self::assertSame([], $result['events']);
        self::assertSame([], $result['links']);
        self::assertArrayNotHasKey('parentSpanId', $result);
    }

    public function test_serialize_span_kind_client() : void
    {
        $span = $this->createSpan(SpanKind::CLIENT);
        $result = $this->serializer->serialize($span);

        self::assertSame(3, $result['kind']);
    }

    public function test_serialize_span_kind_consumer() : void
    {
        $span = $this->createSpan(SpanKind::CONSUMER);
        $result = $this->serializer->serialize($span);

        self::assertSame(5, $result['kind']);
    }

    public function test_serialize_span_kind_producer() : void
    {
        $span = $this->createSpan(SpanKind::PRODUCER);
        $result = $this->serializer->serialize($span);

        self::assertSame(4, $result['kind']);
    }

    public function test_serialize_span_kind_server() : void
    {
        $span = $this->createSpan(SpanKind::SERVER);
        $result = $this->serializer->serialize($span);

        self::assertSame(2, $result['kind']);
    }

    public function test_serialize_span_with_attributes() : void
    {
        $span = $this->createSpan();
        $span->setAttribute('http.method', 'GET');
        $span->setAttribute('http.status_code', 200);

        $result = $this->serializer->serialize($span);

        /** @var array<int, array<string, mixed>> $attributes */
        $attributes = $result['attributes'];
        self::assertCount(2, $attributes);
    }

    public function test_serialize_span_with_end_time() : void
    {
        $span = $this->createSpan();
        $span->end(new \DateTimeImmutable('2024-01-01 12:00:01.000000'));

        $result = $this->serializer->serialize($span);

        self::assertArrayHasKey('endTimeUnixNano', $result);
    }

    public function test_serialize_span_with_error_status() : void
    {
        $span = $this->createSpan();
        $span->setStatus(SpanStatus::error('Something went wrong'));

        $result = $this->serializer->serialize($span);

        /** @var array{code: int, message: string} $status */
        $status = $result['status'];
        self::assertSame(2, $status['code']);
        self::assertSame('Something went wrong', $status['message']);
    }

    public function test_serialize_span_with_events() : void
    {
        $span = $this->createSpan();
        $span->recordEvent(GenericEvent::create('cache.hit', new \DateTimeImmutable(), ['key' => 'user:123']));

        $result = $this->serializer->serialize($span);

        /** @var array<int, array{name: string, timeUnixNano: int, attributes: array<int, array<string, mixed>>}> $events */
        $events = $result['events'];
        self::assertCount(1, $events);
        self::assertSame('cache.hit', $events[0]['name']);
    }

    public function test_serialize_span_with_links() : void
    {
        $span = $this->createSpan();
        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());
        $span->addLink(SpanLink::create($linkedContext, ['reason' => 'batch']));

        $result = $this->serializer->serialize($span);

        /** @var array<int, array{traceId: string, spanId: string, attributes: array<int, array<string, mixed>>}> $links */
        $links = $result['links'];
        self::assertCount(1, $links);
        self::assertSame($linkedContext->traceId->toHex(), $links[0]['traceId']);
        self::assertSame($linkedContext->spanId->toHex(), $links[0]['spanId']);
    }

    public function test_serialize_span_with_ok_status() : void
    {
        $span = $this->createSpan();
        $span->setStatus(SpanStatus::ok());

        $result = $this->serializer->serialize($span);

        /** @var array{code: int, message?: string} $status */
        $status = $result['status'];
        self::assertSame(1, $status['code']);
        self::assertArrayNotHasKey('message', $status);
    }

    public function test_serialize_span_with_parent() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId, $parentSpanId);
        $span = new Span(
            'child-span',
            $context,
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0')
        );

        $result = $this->serializer->serialize($span);

        self::assertSame($parentSpanId->toHex(), $result['parentSpanId']);
    }

    private function createSpan(SpanKind $kind = SpanKind::INTERNAL) : Span
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        return new Span(
            'test-span',
            $context,
            $kind,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0')
        );
    }
}
