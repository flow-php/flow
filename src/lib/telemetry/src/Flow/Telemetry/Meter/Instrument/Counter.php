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
use InvalidArgumentException;
use Psr\Clock\ClockInterface;

use function array_filter;
use function count;
use function is_scalar;

/**
 * Counter instrument for recording non-negative increments.
 *
 * Counters are monotonically increasing - they only go up.
 * Use for counting occurrences: requests, errors, items processed.
 *
 * Example usage:
 * ```php
 * $counter = $meter->createCounter('http.requests', 'requests', 'Total HTTP requests');
 * $counter->add(1, ['http.method' => 'GET']);
 * $counter->add(1, ['http.method' => 'POST']);
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/#counter
 */
final class Counter implements Instrument
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
     * Add a non-negative value to the counter.
     *
     * @param float|int $amount Amount to add (must be >= 0)
     * @param array<string, mixed>|Attributes $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     *
     * @throws \InvalidArgumentException If amount is negative
     */
    public function add(int|float $amount, array|Attributes $attributes = [], ?SpanContext $context = null): void
    {
        if ($amount < 0) {
            throw new InvalidArgumentException('Counter amount must be >= 0, got ' . $amount);
        }

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
                type: MetricType::COUNTER,
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
