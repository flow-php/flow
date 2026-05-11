<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLimits;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatus;
use PHPUnit\Framework\TestCase;

final class SpanTest extends TestCase
{
    public function test_add_link_adds_link(): void
    {
        $span = $this->createSpan();
        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());
        $link = SpanLink::create($linkedContext, ['reason' => 'batch']);

        $result = $span->addLink($link);

        static::assertSame($span, $result);
        static::assertCount(1, $span->links());
        static::assertSame($link, $span->links()[0]);
    }

    public function test_add_multiple_links(): void
    {
        $span = $this->createSpan();
        $link1 = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $link2 = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()));

        $span->addLink($link1)->addLink($link2);

        static::assertCount(2, $span->links());
    }

    public function test_attributes_returns_empty_array_initially(): void
    {
        $span = $this->createSpan();

        static::assertSame([], $span->attributes());
    }

    public function test_constructor_creates_span(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $startTime = new \DateTimeImmutable();
        $scope = new InstrumentationScope('test', '1.0.0');

        $resource = ResourceMother::default();
        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, $resource, $scope);

        static::assertSame('test-span', $span->name());
        static::assertSame($context, $span->context());
        static::assertSame(SpanKind::INTERNAL, $span->kind());
        static::assertSame($startTime, $span->startTime());
        static::assertSame($resource, $span->resource());
        static::assertSame($scope, $span->scope());
        static::assertNull($span->endTime());
        static::assertNull($span->status());
        static::assertSame([], $span->attributes());
        static::assertSame([], $span->events());
        static::assertSame([], $span->links());
        static::assertFalse($span->isEnded());
    }

    public function test_duration_returns_milliseconds(): void
    {
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00.000000');
        $endTime = new \DateTimeImmutable('2024-01-01 12:00:00.500000');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $span = new Span(
            'test-span',
            $context,
            SpanKind::INTERNAL,
            $startTime,
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );

        $span->end($endTime);

        static::assertEqualsWithDelta(500.0, $span->duration(), 0.001);
    }

    public function test_duration_returns_null_when_not_ended(): void
    {
        $span = $this->createSpan();

        static::assertNull($span->duration());
    }

    public function test_end_only_sets_once(): void
    {
        $span = $this->createSpan();
        $firstEnd = new \DateTimeImmutable('2024-01-01 12:00:00');
        $secondEnd = new \DateTimeImmutable('2024-01-01 13:00:00');

        $span->end($firstEnd);
        $span->end($secondEnd);

        static::assertSame($firstEnd, $span->endTime());
    }

    public function test_end_sets_end_time(): void
    {
        $span = $this->createSpan();
        $endTime = new \DateTimeImmutable();

        $result = $span->end($endTime);

        static::assertSame($span, $result);
        static::assertSame($endTime, $span->endTime());
        static::assertTrue($span->isEnded());
    }

    public function test_end_uses_current_time_when_not_provided(): void
    {
        $span = $this->createSpan();
        $before = new \DateTimeImmutable();

        $span->end();

        $after = new \DateTimeImmutable();
        static::assertNotNull($span->endTime());
        static::assertGreaterThanOrEqual($before, $span->endTime());
        static::assertLessThanOrEqual($after, $span->endTime());
    }

    public function test_events_returns_empty_array_initially(): void
    {
        $span = $this->createSpan();

        static::assertSame([], $span->events());
    }

    public function test_fluent_interface_allows_chaining(): void
    {
        $span = $this->createSpan();
        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());

        $result = $span
            ->setAttribute('key1', 'value1')
            ->setAttributes(['key2' => 'value2'])
            ->recordEvent(GenericEvent::create('test.event', new \DateTimeImmutable()))
            ->addLink(SpanLink::create($linkedContext))
            ->setStatus(SpanStatus::ok())
            ->rename('new-name')
            ->end();

        static::assertSame($span, $result);
        static::assertSame('new-name', $span->name());
        static::assertCount(2, $span->attributes());
        static::assertCount(1, $span->events());
        static::assertCount(1, $span->links());
        static::assertNotNull($span->status());
        static::assertTrue($span->isEnded());
    }

    public function test_from_array_with_minimal_data(): void
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

        static::assertSame('minimal-span', $span->name());
        static::assertSame(SpanKind::INTERNAL, $span->kind());
        static::assertNull($span->endTime());
        static::assertNull($span->status());
        static::assertEmpty($span->attributes());
        static::assertEmpty($span->events());
        static::assertEmpty($span->links());
    }

    public function test_limits_attribute_count_allows_overwriting_existing(): void
    {
        $limits = new SpanLimits(attributeCountLimit: 2);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttribute('key1', 'value1');
        $span->setAttribute('key2', 'value2');
        $span->setAttribute('key1', 'updated');

        static::assertCount(2, $span->attributes());
        static::assertSame(0, $span->droppedAttributeCount());
        static::assertSame('updated', $span->attributes()['key1']);
    }

    public function test_limits_attribute_count_drops_excess_attributes(): void
    {
        $limits = new SpanLimits(attributeCountLimit: 3);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttributes([
            'key1' => 'value1',
            'key2' => 'value2',
            'key3' => 'value3',
            'key4' => 'value4',
            'key5' => 'value5',
        ]);

        static::assertCount(3, $span->attributes());
        static::assertSame(2, $span->droppedAttributeCount());
        static::assertArrayHasKey('key1', $span->attributes());
        static::assertArrayHasKey('key2', $span->attributes());
        static::assertArrayHasKey('key3', $span->attributes());
        static::assertArrayNotHasKey('key4', $span->attributes());
    }

    public function test_limits_attribute_count_drops_single_attribute(): void
    {
        $limits = new SpanLimits(attributeCountLimit: 2);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttribute('key1', 'value1');
        $span->setAttribute('key2', 'value2');
        $span->setAttribute('key3', 'value3');

        static::assertCount(2, $span->attributes());
        static::assertSame(1, $span->droppedAttributeCount());
    }

    public function test_limits_attribute_value_length_truncates_array_strings(): void
    {
        $limits = new SpanLimits(attributeValueLengthLimit: 5);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttribute('tags', ['short', 'this-is-long']);

        static::assertSame(['short', 'this-'], $span->attributes()['tags']);
    }

    public function test_limits_attribute_value_length_truncates_strings(): void
    {
        $limits = new SpanLimits(attributeValueLengthLimit: 10);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttribute('short', 'abc');
        $span->setAttribute('long', 'this-is-a-very-long-string');

        static::assertSame('abc', $span->attributes()['short']);
        static::assertSame('this-is-a-', $span->attributes()['long']);
    }

    public function test_limits_dropped_counts_in_normalize(): void
    {
        $limits = new SpanLimits(attributeCountLimit: 2, eventCountLimit: 1, linkCountLimit: 1);
        $span = $this->createSpanWithLimits($limits);

        $span->setAttributes(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);
        $span->recordEvent(GenericEvent::create('e1', new \DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('e2', new \DateTimeImmutable()));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));

        $normalized = $span->normalize();

        static::assertSame(1, $normalized['droppedAttributeCount']);
        static::assertSame(1, $normalized['droppedEventsCount']);
        static::assertSame(1, $normalized['droppedLinksCount']);
    }

    public function test_limits_event_attributes_are_enforced(): void
    {
        $limits = new SpanLimits(attributePerEventCountLimit: 2, attributeValueLengthLimit: 10);
        $span = $this->createSpanWithLimits($limits);

        $event = GenericEvent::create('test.event', new \DateTimeImmutable(), [
            'key1' => 'short',
            'key2' => 'this-is-a-very-long-value',
            'key3' => 'dropped',
        ]);
        $span->recordEvent($event);

        $recordedEvent = $span->events()[0];
        $attrs = $recordedEvent->attributes();
        static::assertCount(2, $attrs);
        static::assertSame('short', $attrs['key1']);
        static::assertSame('this-is-a-', $attrs['key2']);
        static::assertSame(1, $recordedEvent->droppedAttributeCount());
    }

    public function test_limits_event_count_drops_excess_events(): void
    {
        $limits = new SpanLimits(eventCountLimit: 2);
        $span = $this->createSpanWithLimits($limits);

        $span->recordEvent(GenericEvent::create('event1', new \DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('event2', new \DateTimeImmutable()));
        $span->recordEvent(GenericEvent::create('event3', new \DateTimeImmutable()));

        static::assertCount(2, $span->events());
        static::assertSame(1, $span->droppedEventsCount());
        static::assertSame('event1', $span->events()[0]->name());
        static::assertSame('event2', $span->events()[1]->name());
    }

    public function test_limits_link_attributes_are_enforced(): void
    {
        $limits = new SpanLimits(attributePerLinkCountLimit: 2, attributeValueLengthLimit: 10);
        $span = $this->createSpanWithLimits($limits);

        $link = SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()), [
            'key1' => 'short',
            'key2' => 'this-is-a-very-long-value',
            'key3' => 'dropped',
        ]);
        $span->addLink($link);

        $recordedLink = $span->links()[0];
        $attrs = $recordedLink->attributes->normalize();
        static::assertCount(2, $attrs);
        static::assertSame('short', $attrs['key1']);
        static::assertSame('this-is-a-', $attrs['key2']);
        static::assertSame(1, $recordedLink->droppedAttributeCount);
    }

    public function test_limits_link_count_drops_excess_links(): void
    {
        $limits = new SpanLimits(linkCountLimit: 2);
        $span = $this->createSpanWithLimits($limits);

        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));

        static::assertCount(2, $span->links());
        static::assertSame(1, $span->droppedLinksCount());
    }

    public function test_links_returns_empty_array_initially(): void
    {
        $span = $this->createSpan();

        static::assertSame([], $span->links());
    }

    public function test_normalize_from_array_round_trip(): void
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
        $original->recordEvent(GenericEvent::create('event', new \DateTimeImmutable('2024-01-01 12:00:00.500000')));
        $original->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate()), [
            'link.attr' => 'test',
        ]));
        $original->setStatus(SpanStatus::error('Something failed'));
        $original->end($endTime);

        $normalized = $original->normalize();
        $restored = Span::fromArray($normalized);

        static::assertSame($original->name(), $restored->name());
        static::assertSame($original->kind(), $restored->kind());
        static::assertSame($original->context()->traceId->toHex(), $restored->context()->traceId->toHex());
        static::assertSame($original->context()->spanId->toHex(), $restored->context()->spanId->toHex());
        static::assertSame($original->context()->parentSpanId?->toHex(), $restored->context()->parentSpanId?->toHex());
        static::assertEquals($original->startTime(), $restored->startTime());
        static::assertEquals($original->endTime(), $restored->endTime());
        static::assertSame($original->attributes(), $restored->attributes());
        static::assertCount(\count($original->events()), $restored->events());
        static::assertCount(\count($original->links()), $restored->links());
        static::assertSame($original->status()?->code, $restored->status()?->code);
        static::assertSame($original->status()?->description, $restored->status()?->description);
        static::assertSame($original->scope()->name, $restored->scope()->name);
    }

    public function test_normalize_returns_array_representation(): void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();
        $context = SpanContext::create($traceId, $spanId);
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00');
        $endTime = new \DateTimeImmutable('2024-01-01 12:00:01');
        $scope = new InstrumentationScope('test-lib', '1.0.0', 'https://schema.test');

        $span = new Span('test-span', $context, SpanKind::SERVER, $startTime, ResourceMother::default(), $scope);
        $span->setAttribute('http.method', 'GET');
        $span->recordEvent(GenericEvent::create('request.start', new \DateTimeImmutable('2024-01-01 12:00:00.500000')));
        $span->addLink(SpanLink::create(SpanContext::create(TraceId::generate(), SpanId::generate())));
        $span->setStatus(SpanStatus::ok());
        $span->end($endTime);

        $normalized = $span->normalize();

        static::assertSame('test-span', $normalized['name']);
        static::assertSame('server', $normalized['kind']);
        static::assertSame('2024-01-01T12:00:00+00:00', $normalized['startTime']);
        static::assertSame('2024-01-01T12:00:01+00:00', $normalized['endTime']);
        static::assertSame(['http.method' => 'GET'], $normalized['attributes']);
        static::assertCount(1, $normalized['events']);
        static::assertCount(1, $normalized['links']);
        static::assertNotNull($normalized['status']);
        static::assertTrue($normalized['isRecording']);
    }

    public function test_record_event_adds_event(): void
    {
        $span = $this->createSpan();
        $event = GenericEvent::create('test.event', new \DateTimeImmutable(), ['key' => 'value']);

        $result = $span->recordEvent($event);

        static::assertSame($span, $result);
        static::assertCount(1, $span->events());
        static::assertSame($event, $span->events()[0]);
    }

    public function test_record_exception_creates_exception_event(): void
    {
        $span = $this->createSpan();
        $exception = new \RuntimeException('Test exception message');
        $timestamp = new \DateTimeImmutable();

        $result = $span->recordException($exception, $timestamp);

        static::assertSame($span, $result);
        static::assertCount(1, $span->events());

        $event = $span->events()[0];
        static::assertSame('exception', $event->name());
        static::assertSame($timestamp, $event->timestamp());

        $attributes = $event->attributes();
        static::assertSame(\RuntimeException::class, $attributes['exception.type']);
        static::assertSame('Test exception message', $attributes['exception.message']);
        static::assertArrayHasKey('exception.stacktrace', $attributes);
    }

    public function test_record_exception_with_additional_attributes(): void
    {
        $span = $this->createSpan();
        $exception = new \RuntimeException('Error');
        $timestamp = new \DateTimeImmutable();

        $span->recordException($exception, $timestamp, ['custom.key' => 'custom.value']);

        $event = $span->events()[0];
        $attributes = $event->attributes();
        static::assertSame('custom.value', $attributes['custom.key']);
        static::assertSame(\RuntimeException::class, $attributes['exception.type']);
    }

    public function test_record_multiple_events(): void
    {
        $span = $this->createSpan();
        $event1 = GenericEvent::create('event1', new \DateTimeImmutable());
        $event2 = GenericEvent::create('event2', new \DateTimeImmutable());

        $span->recordEvent($event1)->recordEvent($event2);

        static::assertCount(2, $span->events());
    }

    public function test_rename_changes_name(): void
    {
        $span = $this->createSpan('original-name');

        $result = $span->rename('new-name');

        static::assertSame($span, $result);
        static::assertSame('new-name', $span->name());
    }

    public function test_set_attribute_adds_single_attribute(): void
    {
        $span = $this->createSpan();

        $result = $span->setAttribute('user.id', '12345');

        static::assertSame($span, $result);
        static::assertSame('12345', $span->attributes()['user.id']);
    }

    public function test_set_attribute_overwrites_existing(): void
    {
        $span = $this->createSpan();

        $span->setAttribute('key', 'original');
        $span->setAttribute('key', 'overwritten');

        static::assertSame('overwritten', $span->attributes()['key']);
    }

    public function test_set_attribute_supports_various_types(): void
    {
        $span = $this->createSpan();

        $span
            ->setAttribute('string', 'text')
            ->setAttribute('int', 42)
            ->setAttribute('float', 3.14)
            ->setAttribute('bool', true)
            ->setAttribute('array', ['a', 'b', 'c']);

        $attributes = $span->attributes();
        static::assertSame('text', $attributes['string']);
        static::assertSame(42, $attributes['int']);
        static::assertSame(3.14, $attributes['float']);
        static::assertTrue($attributes['bool']);
        static::assertSame(['a', 'b', 'c'], $attributes['array']);
    }

    public function test_set_attributes_adds_multiple_attributes(): void
    {
        $span = $this->createSpan();

        $result = $span->setAttributes([
            'key1' => 'value1',
            'key2' => 'value2',
        ]);

        static::assertSame($span, $result);
        static::assertSame('value1', $span->attributes()['key1']);
        static::assertSame('value2', $span->attributes()['key2']);
    }

    public function test_set_attributes_merges_with_existing(): void
    {
        $span = $this->createSpan();

        $span->setAttribute('existing', 'value');
        $span->setAttributes(['new' => 'value']);

        static::assertCount(2, $span->attributes());
        static::assertSame('value', $span->attributes()['existing']);
        static::assertSame('value', $span->attributes()['new']);
    }

    public function test_set_status_can_be_overwritten(): void
    {
        $span = $this->createSpan();

        $span->setStatus(SpanStatus::unset());
        $span->setStatus(SpanStatus::error('Error occurred'));

        static::assertNotNull($span->status());
        static::assertTrue($span->status()->isError());
        static::assertSame('Error occurred', $span->status()->description);
    }

    public function test_set_status_sets_status(): void
    {
        $span = $this->createSpan();
        $status = SpanStatus::ok();

        $result = $span->setStatus($status);

        static::assertSame($span, $result);
        static::assertSame($status, $span->status());
    }

    public function test_span_with_all_span_kinds(): void
    {
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $startTime = new \DateTimeImmutable();

        foreach (SpanKind::cases() as $kind) {
            $span = new Span(
                'test',
                $context,
                $kind,
                $startTime,
                ResourceMother::default(),
                new InstrumentationScope('test', '1.0.0'),
            );
            static::assertSame($kind, $span->kind());
        }
    }

    private function createSpan(string $name = 'test-span'): Span
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

    private function createSpanWithLimits(SpanLimits $limits, string $name = 'test-span'): Span
    {
        return new Span(
            $name,
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            true,
            $limits,
        );
    }
}
