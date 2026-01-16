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
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = ['user.id' => '12345'];
        $event = new GenericEvent('test.event', $timestamp, Attributes::create($attributes));

        self::assertSame('test.event', $event->name());
        self::assertSame($timestamp, $event->timestamp());
        self::assertSame($attributes, $event->attributes());
    }

    public function test_constructor_creates_event_with_empty_attributes() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $event = new GenericEvent('test.event', $timestamp);

        self::assertSame([], $event->attributes());
    }

    public function test_create_creates_event() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = ['key' => 'value'];
        $event = GenericEvent::create('my.event', $timestamp, $attributes);

        self::assertSame('my.event', $event->name());
        self::assertSame($timestamp, $event->timestamp());
        self::assertSame($attributes, $event->attributes());
    }

    public function test_create_with_attributes() : void
    {
        $timestamp = new \DateTimeImmutable();
        $attributes = ['request.id' => 'abc-123'];
        $event = GenericEvent::create('my.event', $timestamp, $attributes);

        self::assertSame($attributes, $event->attributes());
    }

    public function test_from_array_creates_event() : void
    {
        $data = [
            'name' => 'restored.event',
            'timestamp' => '2024-01-01T12:00:00+00:00',
            'attributes' => ['restored' => true],
        ];

        $event = GenericEvent::fromArray($data);

        self::assertSame('restored.event', $event->name());
        self::assertEquals(new \DateTimeImmutable('2024-01-01T12:00:00+00:00'), $event->timestamp());
        self::assertSame(['restored' => true], $event->attributes());
    }

    public function test_implements_span_event_interface() : void
    {
        $event = GenericEvent::create('test.event', new \DateTimeImmutable());

        self::assertInstanceOf(SpanEvent::class, $event);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $original = GenericEvent::create('round.trip', $timestamp, ['key' => 'value']);

        $normalized = $original->normalize();
        $restored = GenericEvent::fromArray($normalized);

        self::assertSame($original->name(), $restored->name());
        self::assertEquals($original->timestamp(), $restored->timestamp());
        self::assertSame($original->attributes(), $restored->attributes());
    }

    public function test_normalize_returns_array() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01T12:00:00+00:00');
        $event = GenericEvent::create('test.event', $timestamp, ['key' => 'value']);

        self::assertSame([
            'name' => 'test.event',
            'timestamp' => '2024-01-01T12:00:00+00:00',
            'attributes' => ['key' => 'value'],
        ], $event->normalize());
    }

    public function test_supports_array_attribute_values() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = [
            'tags' => ['tag1', 'tag2', 'tag3'],
            'simple' => 'value',
        ];
        $event = GenericEvent::create('test.event', $timestamp, $attributes);

        self::assertSame($attributes, $event->attributes());
    }

    public function test_supports_various_attribute_types() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = [
            'string' => 'text',
            'int' => 42,
            'float' => 3.14,
            'bool' => true,
        ];
        $event = GenericEvent::create('test.event', $timestamp, $attributes);

        self::assertSame($attributes, $event->attributes());
    }
}
