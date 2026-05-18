<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;

/**
 * Interface for events recorded within a span.
 *
 * Events mark significant points in time during a span's lifetime,
 * such as errors, state changes, or notable occurrences.
 */
interface SpanEvent
{
    /**
     * Create a SpanEvent from a normalized array representation.
     *
     * @param array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, droppedAttributeCount?: int} $data Normalized event data
     */
    public static function fromArray(array $data): self;

    /**
     * Get the event attributes.
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public function attributes(): array;

    /**
     * Get the event attributes as an Attributes object.
     */
    public function attributesObject(): Attributes;

    /**
     * Get the count of attributes that were dropped due to limits.
     */
    public function droppedAttributeCount(): int;

    /**
     * Get the event name.
     */
    public function name(): string;

    /**
     * Normalize the event to an array representation for serialization.
     *
     * @return array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, droppedAttributeCount: int}
     */
    public function normalize(): array;

    /**
     * Get the event timestamp.
     */
    public function timestamp(): DateTimeImmutable;
}
