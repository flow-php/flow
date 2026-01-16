<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Meter\{AggregationTemporality, Metric, MetricType};
use Flow\Telemetry\Meter\Exemplar\{ExemplarFilter, ExemplarReservoir, SimpleFixedSizeExemplarReservoir, TraceBasedExemplarFilter};
use Flow\Telemetry\Tracer\SpanContext;
use Psr\Clock\ClockInterface;

/**
 * UpDownCounter instrument for recording increments and decrements.
 *
 * Unlike Counter, UpDownCounter supports negative values and can both
 * increase and decrease. Use for tracking values that can go up and down.
 *
 * Example usage:
 * ```php
 * $counter = $meter->createUpDownCounter('queue.size', 'items', 'Current queue size');
 * $counter->add(5, ['queue.name' => 'tasks']);  // Added 5 items
 * $counter->add(-2, ['queue.name' => 'tasks']); // Removed 2 items
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/#updowncounter
 */
final class UpDownCounter implements Instrument
{
    /**
     * Aggregations by attribute key.
     *
     * @var array<string, array{sum: float|int, attributes: array<string, bool|float|int|string>, reservoir: ExemplarReservoir}>
     */
    private array $aggregations = [];

    /**
     * @param string $name Instrument name
     * @param resource $resource The resource context for this instrument
     * @param InstrumentationScope $scope Instrumentation scope that created this instrument
     * @param ClockInterface $clock Clock for timestamps
     * @param AggregationTemporality $temporality Aggregation temporality
     * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling
     * @param null|string $unit Unit of measurement
     * @param null|string $description Human-readable description
     */
    public function __construct(
        private readonly string $name,
        private readonly Resource $resource,
        private readonly InstrumentationScope $scope,
        private readonly ClockInterface $clock,
        private readonly AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
        private readonly ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
        private readonly ?string $unit = null,
        private readonly ?string $description = null,
    ) {
    }

    /**
     * Add a value to the counter (can be negative).
     *
     * @param float|int $amount Amount to add (positive or negative)
     * @param array<string, bool|float|int|string> $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function add(int|float $amount, array $attributes = [], ?SpanContext $context = null) : void
    {
        $key = $this->attributeKey($attributes);

        if (!isset($this->aggregations[$key])) {
            $this->aggregations[$key] = [
                'sum' => 0,
                'attributes' => $attributes,
                'reservoir' => new SimpleFixedSizeExemplarReservoir(1),
            ];
        }

        $this->aggregations[$key]['sum'] += $amount;

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $amount, $attributes)) {
            $this->aggregations[$key]['reservoir']->offer(
                $amount,
                $attributes,
                $context,
                $this->clock->now(),
            );
        }
    }

    public function collect() : array
    {
        $metrics = [];

        foreach ($this->aggregations as $data) {
            $exemplars = $data['reservoir']->collect();

            $metrics[] = new Metric(
                name: $this->name,
                type: MetricType::UP_DOWN_COUNTER,
                value: $data['sum'],
                attributes: Attributes::create($data['attributes']),
                timestamp: $this->clock->now(),
                resource: $this->resource,
                scope: $this->scope,
                unit: $this->unit,
                description: $this->description,
                temporality: $this->temporality,
                exemplars: $exemplars,
            );
        }

        $this->aggregations = [];

        return $metrics;
    }

    public function description() : ?string
    {
        return $this->description;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function unit() : ?string
    {
        return $this->unit;
    }

    /**
     * Create a unique key from attributes for aggregation lookup.
     *
     * @param array<string, bool|float|int|string> $attributes
     */
    private function attributeKey(array $attributes) : string
    {
        if (\count($attributes) === 0) {
            return '';
        }

        \ksort($attributes);
        $parts = [];

        foreach ($attributes as $key => $value) {
            $parts[] = $key . '=' . (\is_bool($value) ? ($value ? 'true' : 'false') : (string) $value);
        }

        return \implode('|', $parts);
    }
}
