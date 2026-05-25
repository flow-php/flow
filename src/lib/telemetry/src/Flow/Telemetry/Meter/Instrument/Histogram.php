<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Exemplar\AlignedHistogramBucketExemplarReservoir;
use Flow\Telemetry\Meter\Exemplar\ExemplarFilter;
use Flow\Telemetry\Meter\Exemplar\ExemplarReservoir;
use Flow\Telemetry\Meter\Exemplar\TraceBasedExemplarFilter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricLimits;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\SpanContext;
use Psr\Clock\ClockInterface;

use function array_fill;
use function array_filter;
use function array_merge;
use function count;
use function is_scalar;
use function max;
use function min;

/**
 * Histogram instrument for recording value distributions.
 *
 * Histograms track the statistical distribution of measurements,
 * including count, sum, min, max values, and bucket distributions.
 *
 * Example usage:
 * ```php
 * $histogram = $meter->createHistogram('http.request.duration', 'ms', 'Request duration');
 * $histogram->record(42.5, ['http.method' => 'GET']);
 * $histogram->record(128.3, ['http.method' => 'GET']);
 * // On collect: count=2, sum=170.8, min=42.5, max=128.3, plus bucket counts
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/#histogram
 */
final class Histogram implements Instrument
{
    /**
     * Default bucket boundaries (milliseconds, suitable for latency measurements).
     *
     * @var list<float>
     */
    public const array DEFAULT_BOUNDARIES = [
        0.0,
        5.0,
        10.0,
        25.0,
        50.0,
        75.0,
        100.0,
        250.0,
        500.0,
        750.0,
        1000.0,
        2500.0,
        5000.0,
        7500.0,
        10000.0,
    ];

    /**
     * Aggregations by attribute key.
     *
     * @var array<string, array{count: int, sum: float, min: float, max: float, bucketCounts: array<int, int>, reservoir: ExemplarReservoir, attributes: array<string, bool|float|int|string>}>
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
     * @param list<float> $boundaries Explicit bucket boundaries (strictly increasing)
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
        private readonly array $boundaries = self::DEFAULT_BOUNDARIES,
    ) {
        $this->overflowKey = Attributes::create([MetricLimits::OVERFLOW_ATTRIBUTE => true])->id();
    }

    /**
     * Get the configured bucket boundaries.
     *
     * @return array<float>
     */
    public function boundaries(): array
    {
        return $this->boundaries;
    }

    public function collect(): array
    {
        $metrics = [];

        foreach ($this->aggregations as $data) {
            $exemplars = $data['reservoir']->collect();

            $metrics[] = new Metric(
                name: $this->name,
                type: MetricType::HISTOGRAM,
                value: $data['sum'],
                attributes: Attributes::create(array_merge($data['attributes'], [
                    'histogram.count' => $data['count'],
                    'histogram.sum' => $data['sum'],
                    'histogram.min' => $data['min'],
                    'histogram.max' => $data['max'],
                    'histogram.bucketCounts' => $data['bucketCounts'],
                    'histogram.explicitBounds' => $this->boundaries,
                ])),
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

    /**
     * Record a value in the histogram.
     *
     * @param float|int $value Value to record
     * @param array<string, mixed>|Attributes $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function record(int|float $value, array|Attributes $attributes = [], ?SpanContext $context = null): void
    {
        $normalized = $attributes instanceof Attributes ? $attributes->normalize() : $attributes;
        /** @var array<string, bool|float|int|string> $attrs */
        $attrs = array_filter($normalized, static fn($v): bool => is_scalar($v));
        $key = Attributes::create($attrs)->id();
        $floatValue = (float) $value;

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
                'count' => 0,
                'sum' => 0.0,
                'min' => $floatValue,
                'max' => $floatValue,
                'bucketCounts' => array_fill(0, count($this->boundaries) + 1, 0),
                'reservoir' => new AlignedHistogramBucketExemplarReservoir(count($this->boundaries) + 1),
                'attributes' => $attrs,
            ];
        }

        $this->aggregations[$key]['count']++;
        $this->aggregations[$key]['sum'] += $floatValue;
        $this->aggregations[$key]['min'] = min($this->aggregations[$key]['min'], $floatValue);
        $this->aggregations[$key]['max'] = max($this->aggregations[$key]['max'], $floatValue);

        $bucketIndex = $this->findBucketIndex($floatValue);
        $this->aggregations[$key]['bucketCounts'][$bucketIndex]++;

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $floatValue, $attrs)) {
            $this->aggregations[$key]['reservoir']->offer(
                $floatValue,
                $attrs,
                $context,
                $this->clock->now(),
                $bucketIndex,
            );
        }
    }

    public function unit(): ?string
    {
        return $this->unit;
    }

    /**
     * Find the bucket index for a given value.
     *
     * Bucket semantics (per OTLP spec):
     * - Bucket 0: (-∞, bounds[0]]
     * - Bucket i: (bounds[i-1], bounds[i]]
     * - Bucket N: (bounds[N-1], +∞)
     */
    private function findBucketIndex(float $value): int
    {
        foreach ($this->boundaries as $index => $boundary) {
            if ($value <= $boundary) {
                return $index;
            }
        }

        return count($this->boundaries);
    }
}
