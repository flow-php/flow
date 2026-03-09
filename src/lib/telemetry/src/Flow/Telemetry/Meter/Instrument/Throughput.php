<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Meter\{AggregationTemporality, Metric, MetricLimits, MetricType, TimeUnit};
use Flow\Telemetry\Meter\Exemplar\{ExemplarFilter, ExemplarReservoir, SimpleFixedSizeExemplarReservoir, TraceBasedExemplarFilter};
use Flow\Telemetry\Tracer\SpanContext;
use Psr\Clock\ClockInterface;

/**
 * Throughput instrument for tracking rate of items processed over time.
 *
 * Throughput tracks accumulated count and calculates rate (count/time unit).
 * Unlike Gauge which replaces values, Throughput accumulates via add() calls.
 * The timer starts from the first add() call for each attribute combination.
 *
 * Example usage:
 * ```php
 * $throughput = $meter->createThroughput('dataframe_throughput', 'rows', 'Rows processed per second');
 * $throughput->add(100, ['source' => 'csv']); // Process 100 rows
 * $throughput->add(150, ['source' => 'csv']); // Process 150 more rows
 * $metrics = $throughput->collect(); // Returns rate as value (rows/s)
 * ```
 */
final class Throughput implements Instrument
{
    /**
     * Aggregations by attribute key.
     *
     * @var array<string, array{count: int, startTimeNs: float|int, startedAt: \DateTimeImmutable, attributes: array<string, bool|float|int|string>, reservoir: ExemplarReservoir}>
     */
    private array $aggregations = [];

    /**
     * Key for overflow aggregation.
     */
    private readonly string $overflowKey;

    /**
     * @param string $name Instrument name
     * @param resource $resource The resource context for this instrument
     * @param InstrumentationScope $scope Instrumentation scope that created this instrument
     * @param ClockInterface $clock Clock for timestamps
     * @param AggregationTemporality $temporality Aggregation temporality
     * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling
     * @param MetricLimits $limits Cardinality limits for this instrument
     * @param null|string $unit Unit of measurement (e.g., 'rows', 'bytes')
     * @param null|string $description Human-readable description
     * @param null|int $ratePrecision Number of decimal places for rate calculation (default: 2, null for no rounding)
     * @param TimeUnit $timeUnit Time unit for rate calculation (default: SECONDS)
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
        private readonly ?int $ratePrecision = 2,
        private readonly TimeUnit $timeUnit = TimeUnit::SECONDS,
    ) {
        $this->overflowKey = Attributes::create([MetricLimits::OVERFLOW_ATTRIBUTE => true])->id();
    }

    /**
     * Add to the accumulated count.
     *
     * @param int $count Number of items to add
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function add(int $count, array|Attributes $attributes = [], ?SpanContext $context = null) : void
    {
        $normalized = $attributes instanceof Attributes ? $attributes->normalize() : $attributes;
        /** @var array<string, bool|float|int|string> $attrs */
        $attrs = \array_filter($normalized, static fn ($v) : bool => \is_scalar($v));
        $key = Attributes::create($attrs)->id();

        if (!isset($this->aggregations[$key])) {
            $nonOverflowCount = isset($this->aggregations[$this->overflowKey])
                ? \count($this->aggregations) - 1
                : \count($this->aggregations);

            if ($nonOverflowCount >= $this->limits->cardinalityLimit) {
                $key = $this->overflowKey;
                $attrs = [MetricLimits::OVERFLOW_ATTRIBUTE => true];
            }
        }

        if (!isset($this->aggregations[$key])) {
            $this->aggregations[$key] = [
                'count' => 0,
                'startTimeNs' => \hrtime(true),
                'startedAt' => $this->clock->now(),
                'attributes' => $attrs,
                'reservoir' => new SimpleFixedSizeExemplarReservoir(1),
            ];
        }

        $this->aggregations[$key]['count'] += $count;

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $count, $attrs)) {
            $this->aggregations[$key]['reservoir']->offer(
                $count,
                $attrs,
                $context,
                $this->clock->now(),
            );
        }
    }

    public function collect() : array
    {
        $metrics = [];
        $fullUnit = $this->unit !== null
            ? $this->unit . '/' . $this->timeUnit->value
            : null;

        foreach ($this->aggregations as $data) {
            $durationNs = \hrtime(true) - $data['startTimeNs'];
            $durationInTimeUnit = $this->timeUnit->fromNanoseconds($durationNs);
            $rawRate = $durationInTimeUnit > 0
                ? $data['count'] / $durationInTimeUnit
                : 0.0;

            $rate = $this->ratePrecision !== null
                ? \round($rawRate, $this->ratePrecision)
                : $rawRate;

            $exemplars = $data['reservoir']->collect();

            $metrics[] = new Metric(
                name: $this->name,
                type: MetricType::GAUGE,
                value: $rate,
                attributes: Attributes::create($data['attributes']),
                timestamp: $this->clock->now(),
                resource: $this->resource,
                scope: $this->scope,
                unit: $fullUnit,
                description: $this->description,
                temporality: $this->temporality,
                exemplars: $exemplars,
                startTimestamp: $data['startedAt'],
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
        return $this->unit !== null
            ? $this->unit . '/' . $this->timeUnit->value
            : null;
    }
}
