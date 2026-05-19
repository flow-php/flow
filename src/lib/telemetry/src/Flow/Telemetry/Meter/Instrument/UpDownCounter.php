<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Exemplar\ExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\ExemplarReservoir;
use Flow\Telemetry\Meter\Exemplar\SimpleFixedSizeExemplarReservoir;
use Flow\Telemetry\Meter\Exemplar\TraceBasedExemplarFilter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricLimits;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\SpanContext;
use Psr\Clock\ClockInterface;

use function array_filter;
use function count;
use function is_scalar;

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
     * Key for overflow aggregation.
     */
    private readonly string $overflowKey;

    /**
     * @param string $name Instrument name
     * @param \Flow\Telemetry\Resource $resource The resource context for this instrument
     * @param InstrumentationScope $scope Instrumentation scope that created this instrument
     * @param ClockInterface $clock Clock for timestamps
     * @param AggregationTemporality $temporality Aggregation temporality
     * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling
     * @param MetricLimits $limits Cardinality limits for this instrument
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
        private readonly MetricLimits $limits = new MetricLimits(),
        private readonly ?string $unit = null,
        private readonly ?string $description = null,
    ) {
        $this->overflowKey = Attributes::create([MetricLimits::OVERFLOW_ATTRIBUTE => true])->id();
    }

    /**
     * Add a value to the counter (can be negative).
     *
     * @param float|int $amount Amount to add (positive or negative)
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function add(int|float $amount, array|Attributes $attributes = [], ?SpanContext $context = null): void
    {
        $normalized = $attributes instanceof Attributes ? $attributes->normalize() : $attributes;
        /** @var array<string, bool|float|int|string> $attrs */
        $attrs = array_filter($normalized, static fn($v): bool => is_scalar($v));
        $key = Attributes::create($attrs)->id();

        if (!isset($this->aggregations[$key])) {
            $nonOverflowCount = isset($this->aggregations[$this->overflowKey])
                ? count($this->aggregations) - 1
                : count($this->aggregations);

            if ($nonOverflowCount >= $this->limits->cardinalityLimit) {
                $key = $this->overflowKey;
                $attrs = [MetricLimits::OVERFLOW_ATTRIBUTE => true];
            }
        }

        if (!isset($this->aggregations[$key])) {
            $this->aggregations[$key] = [
                'sum' => 0,
                'attributes' => $attrs,
                'reservoir' => new SimpleFixedSizeExemplarReservoir(1),
            ];
        }

        $this->aggregations[$key]['sum'] += $amount;

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $amount, $attrs)) {
            $this->aggregations[$key]['reservoir']->offer($amount, $attrs, $context, $this->clock->now());
        }
    }

    public function collect(): array
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

    public function description(): ?string
    {
        return $this->description;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function unit(): ?string
    {
        return $this->unit;
    }
}
