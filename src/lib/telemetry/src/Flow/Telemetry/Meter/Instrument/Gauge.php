<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Meter\Exemplar\{ExemplarFilter, ExemplarReservoir, SimpleFixedSizeExemplarReservoir, TraceBasedExemplarFilter};
use Flow\Telemetry\Meter\{Metric, MetricType};
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
     * @param string $name Instrument name
     * @param resource $resource The resource context for this instrument
     * @param InstrumentationScope $scope Instrumentation scope that created this instrument
     * @param ClockInterface $clock Clock for timestamps
     * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling
     * @param null|string $unit Unit of measurement
     * @param null|string $description Human-readable description
     */
    public function __construct(
        private readonly string $name,
        private readonly Resource $resource,
        private readonly InstrumentationScope $scope,
        private readonly ClockInterface $clock,
        private readonly ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
        private readonly ?string $unit = null,
        private readonly ?string $description = null,
    ) {
    }

    public function collect() : array
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

    public function description() : ?string
    {
        return $this->description;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * Record a gauge value.
     *
     * @param float|int $value Current value to record
     * @param array<string, bool|float|int|string> $attributes Categorization attributes
     * @param null|SpanContext $context Optional span context for exemplar capture
     */
    public function record(int|float $value, array $attributes = [], ?SpanContext $context = null) : void
    {
        $key = $this->attributeKey($attributes);

        if (!isset($this->aggregations[$key])) {
            $this->aggregations[$key] = [
                'value' => $value,
                'attributes' => $attributes,
                'reservoir' => new SimpleFixedSizeExemplarReservoir(1),
            ];
        } else {
            $this->aggregations[$key]['value'] = $value;
        }

        if ($context !== null && $this->exemplarFilter->shouldSample($context, $value, $attributes)) {
            $this->aggregations[$key]['reservoir']->offer(
                $value,
                $attributes,
                $context,
                $this->clock->now(),
            );
        }
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
