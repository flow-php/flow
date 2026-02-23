<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Attributes;

/**
 * Default implementation of SpanEvent.
 *
 * GenericEvent provides a simple, immutable implementation for recording
 * events within a span. Events are created with explicit timestamps.
 *
 * Example usage:
 * ```php
 * $event = GenericEvent::create('user.login', $clock->now(), ['user.id' => '12345']);
 * echo $event->name(); // "user.login"
 * ```
 *
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final readonly class GenericEvent implements SpanEvent
{
    public function __construct(
        private string $name,
        private \DateTimeImmutable $timestamp,
        private Attributes $attributesObject = new Attributes(),
        private int $droppedAttributeCount = 0,
    ) {
    }

    /**
     * Create an event with an explicit timestamp.
     *
     * @param string $name Event name
     * @param \DateTimeImmutable $timestamp Event timestamp
     * @param Attributes|TAttributeValueMap $attributes Event attributes
     * @param int $droppedAttributeCount Number of attributes dropped due to limits
     */
    public static function create(string $name, \DateTimeImmutable $timestamp, Attributes|array $attributes = [], int $droppedAttributeCount = 0) : self
    {
        return new self($name, $timestamp, $attributes instanceof Attributes ? $attributes : Attributes::create($attributes), $droppedAttributeCount);
    }

    /**
     * Create a GenericEvent from a normalized array representation.
     *
     * @param array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, droppedAttributeCount?: int} $data Normalized event data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            $data['name'],
            new \DateTimeImmutable($data['timestamp']),
            Attributes::fromArray($data['attributes']),
            $data['droppedAttributeCount'] ?? 0,
        );
    }

    /**
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public function attributes() : array
    {
        return $this->attributesObject->normalize();
    }

    public function attributesObject() : Attributes
    {
        return $this->attributesObject;
    }

    public function droppedAttributeCount() : int
    {
        return $this->droppedAttributeCount;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * Normalize the GenericEvent to an array representation for serialization.
     *
     * @return array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, droppedAttributeCount: int}
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'timestamp' => $this->timestamp->format('c'),
            'attributes' => $this->attributesObject->normalize(),
            'droppedAttributeCount' => $this->droppedAttributeCount,
        ];
    }

    public function timestamp() : \DateTimeImmutable
    {
        return $this->timestamp;
    }
}
