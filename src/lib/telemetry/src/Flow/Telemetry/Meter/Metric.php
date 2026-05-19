<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;

/**
 * Represents a single metric measurement.
 *
 * A Metric captures a point-in-time measurement with its metadata,
 * including the instrument type, value, and categorization attributes.
 *
 * Example:
 * ```php
 * $metric = new Metric(
 *     name: 'http.requests',
 *     type: MetricType::COUNTER,
 *     value: 1,
 *     attributes: Attributes::create(['http.method' => 'GET']),
 *     timestamp: new \DateTimeImmutable(),
 *     scope: $instrumentationScope,
 * );
 * ```
 */
final readonly class Metric
{
    /**
     * @param string $name Metric name
     * @param MetricType $type Type of metric instrument
     * @param float|int $value Recorded value
     * @param Attributes $attributes Categorization attributes
     * @param \DateTimeImmutable $timestamp When the measurement was recorded (end of measurement period)
     * @param \Flow\Telemetry\Resource $resource The resource context for this metric
     * @param InstrumentationScope $scope The instrumentation scope that created this metric
     * @param null|string $unit Unit of measurement
     * @param null|string $description Human-readable description
     * @param AggregationTemporality $temporality How the metric is aggregated over time
     * @param array<Exemplar> $exemplars Sample measurements with trace context for drill-down
     * @param null|\DateTimeImmutable $startTimestamp When the measurement period started (for rate/throughput metrics)
     */
    public function __construct(
        public string $name,
        public MetricType $type,
        public int|float $value,
        public Attributes $attributes,
        public DateTimeImmutable $timestamp,
        public Resource $resource,
        public InstrumentationScope $scope,
        public ?string $unit = null,
        public ?string $description = null,
        public AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
        public array $exemplars = [],
        public ?DateTimeImmutable $startTimestamp = null,
    ) {}

    /**
     * Create a Metric from a normalized array representation.
     *
     * @param array{
     *     name: string,
     *     type: string,
     *     value: float|int,
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     timestamp: string,
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     unit: null|string,
     *     description: null|string,
     *     temporality?: int
     * } $data Normalized Metric data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            MetricType::from($data['type']),
            $data['value'],
            Attributes::fromArray($data['attributes']),
            new DateTimeImmutable($data['timestamp']),
            Resource::fromArray($data['resource']),
            InstrumentationScope::fromArray($data['scope']),
            $data['unit'],
            $data['description'],
            isset($data['temporality'])
                ? AggregationTemporality::from($data['temporality'])
                : AggregationTemporality::CUMULATIVE,
        );
    }

    /**
     * Normalize the Metric to an array representation for serialization.
     *
     * @return array{
     *     name: string,
     *     type: string,
     *     value: float|int,
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     timestamp: string,
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     unit: null|string,
     *     description: null|string,
     *     temporality: int
     * }
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'type' => $this->type->value,
            'value' => $this->value,
            'attributes' => $this->attributes->normalize(),
            'timestamp' => $this->timestamp->format('c'),
            'resource' => $this->resource->normalize(),
            'scope' => $this->scope->normalize(),
            'unit' => $this->unit,
            'description' => $this->description,
            'temporality' => $this->temporality->value,
        ];
    }
}
