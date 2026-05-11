<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Meter\Exemplar;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Stores a bounded collection of exemplars for metric aggregation.
 *
 * ExemplarReservoir determines how many and which exemplars to keep
 * during the metric collection period. Different reservoir types
 * implement different sampling strategies appropriate for their
 * associated instrument types.
 *
 * Example usage:
 * ```php
 * $reservoir = new SimpleFixedSizeExemplarReservoir(5);
 * $reservoir->offer(100, ['status' => 'ok'], $spanContext, $timestamp);
 * $exemplars = $reservoir->collect();
 * ```
 */
interface ExemplarReservoir
{
    /**
     * Collect all stored exemplars.
     *
     * @param bool $reset Whether to clear the reservoir after collection
     *
     * @return array<Exemplar>
     */
    public function collect(bool $reset = true): array;

    /**
     * Offer a measurement for potential storage as an exemplar.
     *
     * The reservoir decides whether to keep this measurement based on
     * its sampling strategy. Not all offered measurements will be stored.
     *
     * @param float|int $value The measurement value
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes The measurement attributes
     * @param SpanContext $context The span context for trace correlation
     * @param \DateTimeImmutable $timestamp When the measurement was recorded
     * @param int $bucketIndex For histogram buckets, the bucket index; ignored by other reservoirs
     */
    public function offer(
        int|float $value,
        array $attributes,
        SpanContext $context,
        \DateTimeImmutable $timestamp,
        int $bucketIndex = 0,
    ): void;

    /**
     * Clear all stored exemplars.
     */
    public function reset(): void;
}
