<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use DateTimeImmutable;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;

use function array_merge;
use function is_int;

/**
 * Serializes Metric to OTLP JSON format.
 *
 * Converts Flow Telemetry Metric objects to the OTLP JSON wire format,
 * mapping metric types to their corresponding OTLP representations.
 *
 * @see https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
 */
final readonly class MetricSerializer
{
    private const int AGGREGATION_TEMPORALITY_CUMULATIVE = 2;

    public function __construct(
        private AttributeSerializer $attributeSerializer = new AttributeSerializer(),
    ) {}

    /**
     * Serialize a Metric to OTLP format.
     *
     * @return array<string, mixed>
     */
    public function serialize(Metric $metric): array
    {
        $result = [
            'name' => $metric->name,
        ];

        if ($metric->description !== null) {
            $result['description'] = $metric->description;
        }

        if ($metric->unit !== null) {
            $result['unit'] = $metric->unit;
        }

        $dataPoint = $this->createDataPoint($metric);

        return match ($metric->type) {
            MetricType::COUNTER => array_merge($result, [
                'sum' => [
                    'dataPoints' => [$dataPoint],
                    'aggregationTemporality' => self::AGGREGATION_TEMPORALITY_CUMULATIVE,
                    'isMonotonic' => true,
                ],
            ]),
            MetricType::UP_DOWN_COUNTER => array_merge($result, [
                'sum' => [
                    'dataPoints' => [$dataPoint],
                    'aggregationTemporality' => self::AGGREGATION_TEMPORALITY_CUMULATIVE,
                    'isMonotonic' => false,
                ],
            ]),
            MetricType::GAUGE => array_merge($result, [
                'gauge' => [
                    'dataPoints' => [$dataPoint],
                ],
            ]),
            MetricType::HISTOGRAM => array_merge($result, [
                'histogram' => [
                    'dataPoints' => [$this->createHistogramDataPoint($metric)],
                    'aggregationTemporality' => self::AGGREGATION_TEMPORALITY_CUMULATIVE,
                ],
            ]),
        };
    }

    /**
     * Create a data point for sum/gauge metrics.
     *
     * @return array<string, mixed>
     */
    private function createDataPoint(Metric $metric): array
    {
        $timestamp = $this->toNanoseconds($metric->timestamp);

        $dataPoint = [
            'startTimeUnixNano' => $timestamp,
            'timeUnixNano' => $timestamp,
            'attributes' => $this->attributeSerializer->serialize($metric->attributes),
        ];

        if (is_int($metric->value)) {
            $dataPoint['asInt'] = (string) $metric->value;
        } else {
            $dataPoint['asDouble'] = $metric->value;
        }

        return $dataPoint;
    }

    /**
     * Create a histogram data point.
     *
     * @return array<string, mixed>
     */
    private function createHistogramDataPoint(Metric $metric): array
    {
        $timestamp = $this->toNanoseconds($metric->timestamp);

        return [
            'startTimeUnixNano' => $timestamp,
            'timeUnixNano' => $timestamp,
            'attributes' => $this->attributeSerializer->serialize($metric->attributes),
            'count' => '1',
            'sum' => is_int($metric->value) ? (float) $metric->value : $metric->value,
            'bucketCounts' => [],
            'explicitBounds' => [],
        ];
    }

    /**
     * Convert DateTimeImmutable to nanoseconds since Unix epoch as string.
     */
    private function toNanoseconds(DateTimeImmutable $dateTime): string
    {
        $seconds = (int) $dateTime->format('U');
        $microseconds = (int) $dateTime->format('u');
        $nanoseconds = ($seconds * 1_000_000_000) + ($microseconds * 1_000);

        return (string) $nanoseconds;
    }
}
