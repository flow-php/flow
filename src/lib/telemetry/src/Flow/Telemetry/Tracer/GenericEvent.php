<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Attributes;

/**
 * Default implementation of SpanEvent.
 *
 * GenericEvent provides a simple, immutable implementation for recording
 * events within a span. Events can be created with explicit timestamps
 * or automatically use the current time.
 *
 * Example usage:
 * ```php
 * $event = GenericEvent::now('user.login', ['user.id' => '12345']);
 * echo $event->name(); // "user.login"
 * ```
 */
final readonly class GenericEvent implements SpanEvent
{
    public function __construct(
        private string $name,
        private int $timestamp,
        private Attributes $attributesObject = new Attributes(),
    ) {
    }

    /**
     * Create an event with an explicit timestamp.
     *
     * @param string $name Event name
     * @param int $timestamp Timestamp in nanoseconds since Unix epoch
     * @param array<string, array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable> $attributes Event attributes
     */
    public static function create(string $name, int $timestamp, array $attributes = []) : self
    {
        return new self($name, $timestamp, Attributes::create($attributes));
    }

    /**
     * Create a GenericEvent from a normalized array representation.
     *
     * @param array{name: string, timestamp: int, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>} $data Normalized event data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            $data['name'],
            $data['timestamp'],
            Attributes::fromArray($data['attributes']),
        );
    }

    /**
     * Create an event with the current timestamp.
     *
     * @param string $name Event name
     * @param array<string, array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable> $attributes Event attributes
     */
    public static function now(string $name, array $attributes = []) : self
    {
        return new self($name, \hrtime(true), Attributes::create($attributes));
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

    public function name() : string
    {
        return $this->name;
    }

    /**
     * Normalize the GenericEvent to an array representation for serialization.
     *
     * @return array{name: string, timestamp: int, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'timestamp' => $this->timestamp,
            'attributes' => $this->attributesObject->normalize(),
        ];
    }

    public function timestamp() : int
    {
        return $this->timestamp;
    }
}
