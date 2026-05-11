<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
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

/**
 * Gauge instrument for recording non-additive values.
 *
 * Gauges record the current value at a point in time. Unlike counters,
 * gauge values are not summed - only the last recorded value is kept.
 *
 * Example usage:
 * ```php
 * $gauge = $meter->createGauge('system.memory.usage', 'bytes', 'Current memory usage');
 * $gauge->record(1024 * 1024 * 512, ['host' => 'server-1']);
 * $gauge->record(1024 * 1024 * 480, ['host' => 'server-1']); // Replaces previous
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/#gauge
 */
final class Gauge implements Instrument
{
    /**
     * Aggregations by attribute key (last value only).
     *
     * @var array<string, array{value: float|int, attributes: array<string, bool|float|int|string>, reservoir: ExemplarReservoir}>
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
        private readonly ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
        private readonly MetricLimits $limits = new MetricLimits(),
        private readonly ?string $unit = null,
        private readonly ?string $description = null,
    ) {
        $this->overflowKey = Attributes::create([MetricLimits::OVERFLOW_ATTRIBUTE => true])->id();
    }

    public function collect(): array
    {
        $metrics = [];

        foreach ($this->aggregations as $data) {
            $exemplars = $data['reservoir']->collect();

            $metrics[] = new Metric(
                name: $this->name,
                type: MetricType::GAUGE,
                value: $data['value'],
                attributes: Attributes::create($data['attributes']),
                timestamp: $this->clock->now(),
                resource: $this->resource,
                scope: $this->scope,
                unit: $this->unit,
                description: $this->description,
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
     * Record a gauge value.
     *
     * @param float|int $value Current value to record
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function record(int|float $value, array|Attributes $attributes = [], ?SpanContext $context = null): void
    {
        $normalized = $attributes instanceof Attributes ? $attributes->normalize() : $attributes;
        /** @var array<string, bool|float|int|string> $attrs */
        $attrs = \array_filter($normalized, static fn($v): bool => \is_scalar($v));
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
                'value' => $value,
                'attributes' => $attrs,
                'reservoir' => new SimpleFixedSizeExemplarReservoir(1),
            ];
        } else {
            $this->aggregations[$key]['value'] = $value;
        }

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $value, $attrs)) {
            $this->aggregations[$key]['reservoir']->offer($value, $attrs, $context, $this->clock->now());
        }
    }

    public function unit(): ?string
    {
        return $this->unit;
    }
}
