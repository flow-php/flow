<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Meter\Exemplar;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Fixed-size reservoir using Algorithm R for uniform sampling.
 *
 * This reservoir keeps up to N exemplars, using reservoir sampling
 * to ensure that each measurement has an equal probability of being
 * selected when more than N measurements are offered.
 *
 * This is appropriate for Counter, Gauge, and UpDownCounter instruments
 * where bucket alignment is not meaningful.
 *
 * @see https://en.wikipedia.org/wiki/Reservoir_sampling
 */
final class SimpleFixedSizeExemplarReservoir implements ExemplarReservoir
{
    private int $count = 0;

    /** @var array<int, Exemplar> */
    private array $exemplars = [];

    /**
     * @param int $size Maximum number of exemplars to store (default: 1)
     */
    public function __construct(
        private readonly int $size = 1,
    ) {
        if ($size < 1) {
            throw new \InvalidArgumentException('Reservoir size must be at least 1');
        }
    }

    public function collect(bool $reset = true): array
    {
        $result = \array_values($this->exemplars);

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
    ): void {
        $this->count++;

        $filteredAttributes = $this->filterAttributes($attributes);
        $exemplar = new Exemplar($value, $timestamp, $context->traceId, $context->spanId, $filteredAttributes);

        if (\count($this->exemplars) < $this->size) {
            $this->exemplars[] = $exemplar;

            return;
        }

        if ($this->size === 1) {
            $this->exemplars[0] = $exemplar;

            return;
        }

        $replaceIndex = \random_int(0, $this->count - 1);

        if ($replaceIndex < $this->size) {
            $this->exemplars[$replaceIndex] = $exemplar;
        }
    }

    public function reset(): void
    {
        $this->exemplars = [];
        $this->count = 0;
    }

    /**
     * Filter attributes to only include simple scalar values.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     *
     * @return array<string, bool|float|int|string>
     */
    private function filterAttributes(array $attributes): array
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
