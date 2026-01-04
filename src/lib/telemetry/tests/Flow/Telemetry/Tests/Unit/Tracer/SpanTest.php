<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{GenericEvent, Span, SpanContext, SpanKind, SpanLink, SpanStatus};
use PHPUnit\Framework\TestCase;

final class SpanTest extends TestCase
{
    public function test_add_link_adds_link() : void
    {
        $span = $this->createSpan();
        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());
        $link = SpanLink::create($linkedContext, ['reason' => 'batch']);

        $result = $span->addLink($link);

        self::assertSame($span, $result);
        self::assertCount(1, $span->links());
        self::assertSame($link, $span->links()[0]);
    }

    public function test_add_multiple_links() : void
    {
        $span = $this->createSpan();
        $link1 = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $link2 = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()));

        $span->addLink($link1)->addLink($link2);

        self::assertCount(2, $span->links());
    }

    public function test_attributes_returns_empty_array_initially() : void
    {
        $span = $this->createSpan();

        self::assertSame([], $span->attributes());
    }

    public function test_constructor_creates_span() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $startTime = new \DateTimeImmutable();
        $scope = new InstrumentationScope('test', '1.0.0');

        $resource = ResourceMother::default();
        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, $resource, $scope);

        self::assertSame('test-span', $span->name());
        self::assertSame($context, $span->context());
        self::assertSame(SpanKind::INTERNAL, $span->kind());
        self::assertSame($startTime, $span->startTime());
        self::assertSame($resource, $span->resource());
        self::assertSame($scope, $span->scope());
        self::assertNull($span->endTime());
        self::assertNull($span->status());
        self::assertSame([], $span->attributes());
        self::assertSame([], $span->events());
        self::assertSame([], $span->links());
        self::assertFalse($span->isEnded());
    }

    public function test_duration_returns_milliseconds() : void
    {
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00.000000');
        $endTime = new \DateTimeImmutable('2024-01-01 12:00:00.500000');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, ResourceMother::default(), new InstrumentationScope('test', '1.0.0'));

        $span->end($endTime);

        self::assertEqualsWithDelta(500.0, $span->duration(), 0.001);
    }

    public function test_duration_returns_null_when_not_ended() : void
    {
        $span = $this->createSpan();

        self::assertNull($span->duration());
    }

    public function test_end_only_sets_once() : void
    {
        $span = $this->createSpan();
        $firstEnd = new \DateTimeImmutable('2024-01-01 12:00:00');
        $secondEnd = new \DateTimeImmutable('2024-01-01 13:00:00');

        $span->end($firstEnd);
        $span->end($secondEnd);

        self::assertSame($firstEnd, $span->endTime());
    }

    public function test_end_sets_end_time() : void
    {
        $span = $this->createSpan();
        $endTime = new \DateTimeImmutable();

        $result = $span->end($endTime);

        self::assertSame($span, $result);
        self::assertSame($endTime, $span->endTime());
        self::assertTrue($span->isEnded());
    }

    public function test_end_uses_current_time_when_not_provided() : void
    {
        $span = $this->createSpan();
        $before = new \DateTimeImmutable();

        $span->end();

        $after = new \DateTimeImmutable();
        self::assertNotNull($span->endTime());
        self::assertGreaterThanOrEqual($before, $span->endTime());
        self::assertLessThanOrEqual($after, $span->endTime());
    }

    public function test_events_returns_empty_array_initially() : void
    {
        $span = $this->createSpan();

        self::assertSame([], $span->events());
    }

    public function test_fluent_interface_allows_chaining() : void
    {
        $span = $this->createSpan();
        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());

        $result = $span
            ->setAttribute('key1', 'value1')
            ->setAttributes(['key2' => 'value2'])
            ->recordEvent(GenericEvent::now('test.event'))
            ->addLink(SpanLink::create($linkedContext))
            ->setStatus(SpanStatus::ok())
            ->rename('new-name')
            ->end();

        self::assertSame($span, $result);
        self::assertSame('new-name', $span->name());
        self::assertCount(2, $span->attributes());
        self::assertCount(1, $span->events());
        self::assertCount(1, $span->links());
        self::assertNotNull($span->status());
        self::assertTrue($span->isEnded());
    }

    public function test_from_array_with_minimal_data() : void
    {
        $data = [
            'name' => 'minimal-span',
            'context' => [
                'traceId' => ['hex' => 'a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8'],
                'spanId' => ['hex' => 'a1b2c3d4e5f6a7b8'],
                'parentSpanId' => null,
                'isRemote' => false,
                'traceFlags' => ['byte' => 1],
                'traceState' => ['entries' => []],
            ],
            'kind' => 'internal',
            'startTime' => '2024-01-01T12:00:00+00:00',
            'endTime' => null,
            'resource' => [
                'attributes' => [],
            ],
            'scope' => [
                'name' => 'test',
                'version' => '1.0.0',
                'schemaUrl' => null,
                'attributes' => [],
            ],
            'attributes' => [],
            'events' => [],
            'links' => [],
            'status' => null,
            'isRecording' => true,
        ];

        $span = Span::fromArray($data);

        self::assertSame('minimal-span', $span->name());
        self::assertSame(SpanKind::INTERNAL, $span->kind());
        self::assertNull($span->endTime());
        self::assertNull($span->status());
        self::assertEmpty($span->attributes());
        self::assertEmpty($span->events());
        self::assertEmpty($span->links());
    }

    public function test_links_returns_empty_array_initially() : void
    {
        $span = $this->createSpan();

        self::assertSame([], $span->links());
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $parentSpanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId, $parentSpanId);
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00');
        $endTime = new \DateTimeImmutable('2024-01-01 12:00:01');
        $scope = new InstrumentationScope('test-lib', '1.0.0');

        $original = new Span('test-span', $context, SpanKind::CLIENT, $startTime, ResourceMother::default(), $scope);
        $original->setAttribute('key', 'value');
        $original->recordEvent(GenericEvent::create('event', 1000));
        $original->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()), ['link.attr' => 'test']));
        $original->setStatus(SpanStatus::error('Something failed'));
        $original->end($endTime);

        $normalized = $original->normalize();
        $restored = Span::fromArray($normalized);

        self::assertSame($original->name(), $restored->name());
        self::assertSame($original->kind(), $restored->kind());
        self::assertSame($original->context()->traceId->toHex(), $restored->context()->traceId->toHex());
        self::assertSame($original->context()->spanId->toHex(), $restored->context()->spanId->toHex());
        self::assertSame($original->context()->parentSpanId?->toHex(), $restored->context()->parentSpanId?->toHex());
        self::assertEquals($original->startTime(), $restored->startTime());
        self::assertEquals($original->endTime(), $restored->endTime());
        self::assertSame($original->attributes(), $restored->attributes());
        self::assertCount(\count($original->events()), $restored->events());
        self::assertCount(\count($original->links()), $restored->links());
        self::assertSame($original->status()?->code, $restored->status()?->code);
        self::assertSame($original->status()?->description, $restored->status()?->description);
        self::assertSame($original->scope()->name, $restored->scope()->name);
    }

    public function test_normalize_returns_array_representation() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId);
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00');
        $endTime = new \DateTimeImmutable('2024-01-01 12:00:01');
        $scope = new InstrumentationScope('test-lib', '1.0.0', 'https://schema.test');

        $span = new Span('test-span', $context, SpanKind::SERVER, $startTime, ResourceMother::default(), $scope);
        $span->setAttribute('http.method', 'GET');
        $span->recordEvent(GenericEvent::create('request.start', 1000000));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->setStatus(SpanStatus::ok());
        $span->end($endTime);

        $normalized = $span->normalize();

        self::assertSame('test-span', $normalized['name']);
        self::assertSame('server', $normalized['kind']);
        self::assertSame('2024-01-01T12:00:00+00:00', $normalized['startTime']);
        self::assertSame('2024-01-01T12:00:01+00:00', $normalized['endTime']);
        self::assertSame(['http.method' => 'GET'], $normalized['attributes']);
        self::assertCount(1, $normalized['events']);
        self::assertCount(1, $normalized['links']);
        self::assertNotNull($normalized['status']);
        self::assertTrue($normalized['isRecording']);
    }

    public function test_record_event_adds_event() : void
    {
        $span = $this->createSpan();
        $event = GenericEvent::now('test.event', ['key' => 'value']);

        $result = $span->recordEvent($event);

        self::assertSame($span, $result);
        self::assertCount(1, $span->events());
        self::assertSame($event, $span->events()[0]);
    }

    public function test_record_exception_creates_exception_event() : void
    {
        $span = $this->createSpan();
        $exception = new \RuntimeException('Test exception message');

        $result = $span->recordException($exception);

        self::assertSame($span, $result);
        self::assertCount(1, $span->events());

        $event = $span->events()[0];
        self::assertSame('exception', $event->name());

        $attributes = $event->attributes();
        self::assertSame(\RuntimeException::class, $attributes['exception.type']);
        self::assertSame('Test exception message', $attributes['exception.message']);
        self::assertArrayHasKey('exception.stacktrace', $attributes);
    }

    public function test_record_exception_with_additional_attributes() : void
    {
        $span = $this->createSpan();
        $exception = new \RuntimeException('Error');

        $span->recordException($exception, ['custom.key' => 'custom.value']);

        $event = $span->events()[0];
        $attributes = $event->attributes();
        self::assertSame('custom.value', $attributes['custom.key']);
        self::assertSame(\RuntimeException::class, $attributes['exception.type']);
    }

    public function test_record_multiple_events() : void
    {
        $span = $this->createSpan();
        $event1 = GenericEvent::now('event1');
        $event2 = GenericEvent::now('event2');

        $span->recordEvent($event1)->recordEvent($event2);

        self::assertCount(2, $span->events());
    }

    public function test_rename_changes_name() : void
    {
        $span = $this->createSpan('original-name');

        $result = $span->rename('new-name');

        self::assertSame($span, $result);
        self::assertSame('new-name', $span->name());
    }

    public function test_set_attribute_adds_single_attribute() : void
    {
        $span = $this->createSpan();

        $result = $span->setAttribute('user.id', '12345');

        self::assertSame($span, $result);
        self::assertSame('12345', $span->attributes()['user.id']);
    }

    public function test_set_attribute_overwrites_existing() : void
    {
        $span = $this->createSpan();

        $span->setAttribute('key', 'original');
        $span->setAttribute('key', 'overwritten');

        self::assertSame('overwritten', $span->attributes()['key']);
    }

    public function test_set_attribute_supports_various_types() : void
    {
        $span = $this->createSpan();

        $span
            ->setAttribute('string', 'text')
            ->setAttribute('int', 42)
            ->setAttribute('float', 3.14)
            ->setAttribute('bool', true)
            ->setAttribute('array', ['a', 'b', 'c']);

        $attributes = $span->attributes();
        self::assertSame('text', $attributes['string']);
        self::assertSame(42, $attributes['int']);
        self::assertSame(3.14, $attributes['float']);
        self::assertTrue($attributes['bool']);
        self::assertSame(['a', 'b', 'c'], $attributes['array']);
    }

    public function test_set_attributes_adds_multiple_attributes() : void
    {
        $span = $this->createSpan();

        $result = $span->setAttributes([
            'key1' => 'value1',
            'key2' => 'value2',
        ]);

        self::assertSame($span, $result);
        self::assertSame('value1', $span->attributes()['key1']);
        self::assertSame('value2', $span->attributes()['key2']);
    }

    public function test_set_attributes_merges_with_existing() : void
    {
        $span = $this->createSpan();

        $span->setAttribute('existing', 'value');
        $span->setAttributes(['new' => 'value']);

        self::assertCount(2, $span->attributes());
        self::assertSame('value', $span->attributes()['existing']);
        self::assertSame('value', $span->attributes()['new']);
    }

    public function test_set_status_can_be_overwritten() : void
    {
        $span = $this->createSpan();

        $span->setStatus(SpanStatus::unset());
        $span->setStatus(SpanStatus::error('Error occurred'));

        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isError());
        self::assertSame('Error occurred', $span->status()->description);
    }

    public function test_set_status_sets_status() : void
    {
        $span = $this->createSpan();
        $status = SpanStatus::ok();

        $result = $span->setStatus($status);

        self::assertSame($span, $result);
        self::assertSame($status, $span->status());
    }

    public function test_span_with_all_span_kinds() : void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $startTime = new \DateTimeImmutable();

        foreach (SpanKind::cases() as $kind) {
            $span = new Span('test', $context, $kind, $startTime, ResourceMother::default(), new InstrumentationScope('test', '1.0.0'));
            self::assertSame($kind, $span->kind());
        }
    }

    private function createSpan(string $name = 'test-span') : Span
    {
        return new Span(
            $name,
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
