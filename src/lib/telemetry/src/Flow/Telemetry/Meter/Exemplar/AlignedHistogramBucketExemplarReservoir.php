<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Meter\Exemplar;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Histogram-aligned reservoir keeping one exemplar per bucket.
 *
 * This reservoir stores at most one exemplar per histogram bucket,
 * providing representative samples across the entire value range.
 * When multiple measurements fall into the same bucket, the latest
 * one is kept.
 *
 * This is appropriate for Histogram instruments where bucket alignment
 * provides meaningful samples across the value distribution.
 */
final class AlignedHistogramBucketExemplarReservoir implements ExemplarReservoir
{
    /** @var array<int, null|Exemplar> */
    private array $buckets;

    /**
     * @param int $bucketCount Number of histogram buckets (including overflow)
     */
    public function __construct(
        private readonly int $bucketCount,
    ) {
        if ($bucketCount < 1) {
            throw new \InvalidArgumentException('Bucket count must be at least 1');
        }

        $this->buckets = \array_fill(0, $bucketCount, null);
    }

    public function collect(bool $reset = true) : array
    {
        $result = [];

        foreach ($this->buckets as $exemplar) {
            if ($exemplar !== null) {
                $result[] = $exemplar;
            }
        }

        if ($reset) {
            $this->reset();
        }

        return $result;
    }

    public function offer(
        int|float $value,
        array $attributes,
        SpanContext $context,
        \DateTimeImmutable $timestamp,
        int $bucketIndex = 0,
    ) : void {
        if ($bucketIndex < 0 || $bucketIndex >= $this->bucketCount) {
            return;
        }

        $filteredAttributes = $this->filterAttributes($attributes);

        $this->buckets[$bucketIndex] = new Exemplar(
            $value,
            $timestamp,
            $context->traceId,
            $context->spanId,
            $filteredAttributes,
        );
    }

    public function reset() : void
    {
        $this->buckets = \array_fill(0, $this->bucketCount, null);
    }

    /**
     * Filter attributes to only include simple scalar values.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     *
     * @return array<string, bool|float|int|string>
     */
    private function filterAttributes(array $attributes) : array
    {
        $filtered = [];

        foreach ($attributes as $key => $value) {
            if (!\is_array($value)) {
                $filtered[$key] = $value;
            }
        }

        return $filtered;
    }
}
