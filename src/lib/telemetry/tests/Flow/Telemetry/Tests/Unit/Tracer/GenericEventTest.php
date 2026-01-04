<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Tracer\{GenericEvent, SpanEvent};
use PHPUnit\Framework\TestCase;

final class GenericEventTest extends TestCase
{
    public function test_constructor_creates_event() : void
    {
        $attributes = ['user.id' => '12345'];
        $event = new GenericEvent('test.event', 1234567890, Attributes::create($attributes));

        self::assertSame('test.event', $event->name());
        self::assertSame(1234567890, $event->timestamp());
        self::assertSame($attributes, $event->attributes());
    }

    public function test_constructor_creates_event_with_empty_attributes() : void
    {
        $event = new GenericEvent('test.event', 1234567890);

        self::assertSame([], $event->attributes());
    }

    public function test_create_creates_event() : void
    {
        $attributes = ['key' => 'value'];
        $event = GenericEvent::create('my.event', 9876543210, $attributes);

        self::assertSame('my.event', $event->name());
        self::assertSame(9876543210, $event->timestamp());
        self::assertSame($attributes, $event->attributes());
    }

    public function test_from_array_creates_event() : void
    {
        $data = [
            'name' => 'restored.event',
            'timestamp' => 1111111111,
            'attributes' => ['restored' => true],
        ];

        $event = GenericEvent::fromArray($data);

        self::assertSame('restored.event', $event->name());
        self::assertSame(1111111111, $event->timestamp());
        self::assertSame(['restored' => true], $event->attributes());
    }

    public function test_implements_span_event_interface() : void
    {
        $event = GenericEvent::now('test.event');

        self::assertInstanceOf(SpanEvent::class, $event);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $original = GenericEvent::create('round.trip', 5555555555, ['key' => 'value']);

        $normalized = $original->normalize();
        $restored = GenericEvent::fromArray($normalized);

        self::assertSame($original->name(), $restored->name());
        self::assertSame($original->timestamp(), $restored->timestamp());
        self::assertSame($original->attributes(), $restored->attributes());
    }

    public function test_normalize_returns_array() : void
    {
        $event = GenericEvent::create('test.event', 1234567890, ['key' => 'value']);

        self::assertSame([
            'name' => 'test.event',
            'timestamp' => 1234567890,
            'attributes' => ['key' => 'value'],
        ], $event->normalize());
    }

    public function test_now_creates_event_with_attributes() : void
    {
        $attributes = ['request.id' => 'abc-123'];
        $event = GenericEvent::now('now.event', $attributes);

        self::assertSame($attributes, $event->attributes());
    }

    public function test_now_creates_event_with_current_timestamp() : void
    {
        $before = \hrtime(true);
        $event = GenericEvent::now('now.event');
        $after = \hrtime(true);

        self::assertSame('now.event', $event->name());
        self::assertGreaterThanOrEqual($before, $event->timestamp());
        self::assertLessThanOrEqual($after, $event->timestamp());
        self::assertSame([], $event->attributes());
    }

    public function test_supports_array_attribute_values() : void
    {
        $attributes = [
            'tags' => ['tag1', 'tag2', 'tag3'],
            'simple' => 'value',
        ];
        $event = GenericEvent::create('test.event', 1234567890, $attributes);

        self::assertSame($attributes, $event->attributes());
    }

    public function test_supports_various_attribute_types() : void
    {
        $attributes = [
            'string' => 'text',
            'int' => 42,
            'float' => 3.14,
            'bool' => true,
        ];
        $event = GenericEvent::create('test.event', 1234567890, $attributes);

        self::assertSame($attributes, $event->attributes());
    }
}
